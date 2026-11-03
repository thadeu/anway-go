package anway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	defaultConcurrency = 10
	defaultShutdown    = 25 * time.Second
	defaultCancelGrace = 5 * time.Second
	beatInterval       = 15 * time.Second
	errorPause         = time.Second
	emptyFetchMinimum  = 200 * time.Millisecond
	backtraceLines     = 30
)

var errJobAbandoned = errors.New("the worker stopped before the job finished")

// Handler runs one job.
type Handler func(ctx context.Context, job *Job) error

// Middleware wraps a handler.
type Middleware func(next Handler) Handler

// WorkerOptions changes a worker. All fields are optional.
type WorkerOptions struct {
	Queues          []string
	Concurrency     int
	ShutdownTimeout time.Duration
	CancelGrace     time.Duration
	Labels          []string
	BeatInterval    time.Duration
	Logger          *slog.Logger
	Client          Options
}

// Worker fetches and runs jobs.
type Worker struct {
	url           string
	options       WorkerOptions
	handlers      map[string]Handler
	middleware    []Middleware
	subscriptions []Subscription
}

// NewWorker creates a worker. A blank URL uses ANWAY_URL. Without queues, the
// worker fetches from the queues of its subscriptions, or from "default" when
// it has no subscription.
func NewWorker(serverURL string, options WorkerOptions) *Worker {
	options.Queues = slices.Clone(options.Queues)

	if options.Concurrency <= 0 {
		options.Concurrency = defaultConcurrency
	}

	if options.ShutdownTimeout <= 0 {
		options.ShutdownTimeout = defaultShutdown
	}

	if options.CancelGrace <= 0 {
		options.CancelGrace = defaultCancelGrace
	}

	if options.BeatInterval <= 0 {
		options.BeatInterval = beatInterval
	}

	if options.Logger == nil {
		options.Logger = slog.Default()
	}

	return &Worker{url: serverURL, options: options, handlers: map[string]Handler{}}
}

// Register connects a job type to a handler.
func (w *Worker) Register(jobType string, handler Handler) {
	w.handlers[jobType] = handler
}

// Subscribe connects the job type of a subscription to a handler, and
// subscribes it to its topic. The worker fetches from the queue, and sends
// SUBSCRIBE when Run starts, before its first FETCH. Retry and ReserveFor are
// the options of each copy. Each start sends them again, so the code of the
// worker sets them, as in the Ruby and JS clients.
func (w *Worker) Subscribe(subscription Subscription, handler Handler) {
	w.Register(subscription.JobType, handler)

	w.subscriptions = append(w.subscriptions, subscription)

	if !slices.Contains(w.options.Queues, subscription.Queue) {
		w.options.Queues = append(w.options.Queues, subscription.Queue)
	}
}

// Use adds middleware. The first one added is the outermost.
func (w *Worker) Use(middleware ...Middleware) {
	w.middleware = append(w.middleware, middleware...)
}

// session is the state of one Run call.
type session struct {
	client     *Client
	queues     []string
	jobCtx     context.Context
	cancelJobs context.CancelFunc
	stop       chan struct{}
	stopOnce   sync.Once
	finished   chan struct{}

	mu      sync.Mutex
	running map[string]*Job
}

func (s *session) stopFetching() {
	s.stopOnce.Do(func() { close(s.stop) })
}

func (s *session) stopped() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

// pause waits, and ends early when fetching stops.
func (s *session) pause(duration time.Duration) {
	select {
	case <-s.stop:
	case <-time.After(duration):
	}
}

func (s *session) track(job *Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.running[job.JID] = job
}

// claim gives one caller the right to report a job.
func (s *session) claim(jid string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, found := s.running[jid]
	delete(s.running, jid)

	return found
}

func (s *session) remaining() []*Job {
	s.mu.Lock()
	defer s.mu.Unlock()

	jobs := make([]*Job, 0, len(s.running))

	for _, job := range s.running {
		jobs = append(jobs, job)
	}

	return jobs
}

// Run works until ctx ends or the server sends terminate. After quiet, the
// worker stops fetching and stays alive until one of these occurs.
//
// At a stop, running jobs get ShutdownTimeout to finish. Then their contexts
// are canceled, and they get CancelGrace to return. The worker then fails jobs
// that still run and returns, so a handler that ignores its context cannot
// block the shutdown.
func (w *Worker) Run(ctx context.Context) error {
	wid, err := randomID()
	if err != nil {
		return err
	}

	options := w.options.Client
	options.WID = wid
	options.Labels = append([]string{"go"}, w.options.Labels...)
	options.PoolSize = w.options.Concurrency + 1

	client, err := Open(w.url, options)
	if err != nil {
		return err
	}
	defer client.Close()

	if _, err := client.Beat(ctx, ""); err != nil && ctx.Err() == nil {
		return err
	}

	if err := w.subscribe(ctx, client); err != nil && ctx.Err() == nil {
		return err
	}

	queues := w.options.Queues

	if len(queues) == 0 {
		queues = []string{"default"}
	}

	jobCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	current := &session{
		client:     client,
		queues:     queues,
		jobCtx:     jobCtx,
		cancelJobs: cancelJobs,
		stop:       make(chan struct{}),
		finished:   make(chan struct{}),
		running:    map[string]*Job{},
	}

	var group sync.WaitGroup

	for range w.options.Concurrency {
		group.Go(func() { w.fetchLoop(current) })
	}

	go func() {
		group.Wait()
		close(current.finished)
	}()

	ticker := time.NewTicker(w.options.BeatInterval)
	defer ticker.Stop()

	state := ""

	for {
		select {
		case <-ctx.Done():
			w.shutdown(current)

			return nil
		case <-ticker.C:
			next, err := client.Beat(ctx, state)
			if err != nil {
				w.options.Logger.Warn("beat failed", "error", err)

				continue
			}

			if next == "quiet" {
				state = next
				current.stopFetching()
			}

			if next == "terminate" {
				w.shutdown(current)

				return nil
			}
		}
	}
}

// subscribe sends SUBSCRIBE for each subscription of the worker. The server
// keeps one subscription for the same topic, queue, and job type.
func (w *Worker) subscribe(ctx context.Context, client *Client) error {
	for _, subscription := range w.subscriptions {
		if err := client.Subscribe(ctx, subscription); err != nil {
			return fmt.Errorf("subscribe %s to %s: %w", subscription.JobType, subscription.Topic, err)
		}
	}

	return nil
}

func (w *Worker) shutdown(current *session) {
	current.stopFetching()

	if waitFinished(current.finished, w.options.ShutdownTimeout) {
		return
	}

	current.cancelJobs()

	if waitFinished(current.finished, w.options.CancelGrace) {
		return
	}

	for _, job := range current.remaining() {
		if !current.claim(job.JID) {
			continue
		}

		w.options.Logger.Warn("job did not stop", "jid", job.JID, "jobtype", job.Type)

		w.report(current.client, job, errJobAbandoned, nil)
	}
}

func waitFinished(finished <-chan struct{}, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-finished:
		return true
	case <-timer.C:
		return false
	}
}

// fetchLoop never cancels a fetch in progress. A canceled fetch can lose its
// reply, and the job then waits for its reservation to expire.
func (w *Worker) fetchLoop(current *session) {
	for !current.stopped() {
		started := time.Now()

		job, err := current.client.Fetch(context.Background(), current.queues...)
		if err != nil {
			w.options.Logger.Warn("fetch failed", "error", err)
			current.pause(errorPause)

			continue
		}

		if job != nil {
			w.process(current, job)

			continue
		}

		if time.Since(started) < emptyFetchMinimum {
			current.pause(emptyFetchMinimum)
		}
	}
}

func (w *Worker) process(current *session, job *Job) {
	current.track(job)

	err := w.invoke(current.jobCtx, job)

	if !current.claim(job.JID) {
		return
	}

	w.report(current.client, job, err, panicBacktrace(err))
}

func (w *Worker) report(client *Client, job *Job, jobErr error, backtrace []string) {
	ctx, cancel := context.WithTimeout(context.Background(), client.options.Timeout)
	defer cancel()

	var err error

	if jobErr == nil {
		err = client.Ack(ctx, job.JID)
	} else {
		err = client.Fail(ctx, job.JID, errorType(jobErr), jobErr.Error(), backtrace)
	}

	if err != nil {
		w.options.Logger.Warn("job report failed, the reservation will expire", "jid", job.JID, "error", err)
	}
}

func panicBacktrace(err error) []string {
	var panicked *panicError

	if errors.As(err, &panicked) {
		return panicked.backtrace
	}

	return nil
}

func (w *Worker) invoke(ctx context.Context, job *Job) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = newPanicError(value)
		}
	}()

	handler, known := w.handlers[job.Type]
	if !known {
		return fmt.Errorf("no handler for job type %q", job.Type)
	}

	for i := len(w.middleware) - 1; i >= 0; i-- {
		handler = w.middleware[i](handler)
	}

	return handler(ctx, job)
}

type panicError struct {
	message   string
	backtrace []string
}

func (e *panicError) Error() string {
	return e.message
}

func newPanicError(value any) *panicError {
	lines := strings.Split(strings.TrimSpace(string(debug.Stack())), "\n")

	return &panicError{
		message:   fmt.Sprintf("panic: %v", value),
		backtrace: lines[:min(backtraceLines, len(lines))],
	}
}

func errorType(err error) string {
	return strings.TrimPrefix(fmt.Sprintf("%T", err), "*")
}
