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
	"sync/atomic"
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
	maxThreads         = 1000
)

var errJobAbandoned = errors.New("the worker stopped before the job finished")

// Handler runs one job.
type Handler func(ctx context.Context, job *Job) error

// Middleware wraps a handler.
type Middleware func(next Handler) Handler

// WorkerOptions changes a worker. All fields are optional.
//
// Concurrency is the number of jobs that the worker runs at once (default
// 10). The server can change it while the worker runs, from the web UI, from
// the HTTP API, or with autoscale. MaxConcurrency is the most jobs that the
// worker can run at once, such as the size of a database pool. The worker
// never runs more, and the server never asks for more. 0 is no limit.
// Autoscale starts the worker with autoscale on.
//
// The web UI shows the worker by Hostname and Tag. A blank Hostname is the
// machine name. The server saves a change of the threads for this name, so
// keep it stable across deploys. Tag is a short name for the app or the role
// of the process, such as "shop".
type WorkerOptions struct {
	Queues          []string
	Concurrency     int
	MaxConcurrency  int
	Autoscale       *Autoscale
	Hostname        string
	Tag             string
	ShutdownTimeout time.Duration
	CancelGrace     time.Duration
	Labels          []string
	BeatInterval    time.Duration
	Logger          *slog.Logger
	Client          ClientOptions
}

// Autoscale lets the server change the threads of the worker between Min and
// Max, from the load of its queues. A zero Min is the Concurrency of the
// worker. The server lowers a Max above MaxConcurrency to it.
type Autoscale struct {
	Min int `json:"min,omitempty"`
	Max int `json:"max"`
}

func (a *Autoscale) check() error {
	if a == nil {
		return nil
	}

	if a.Max < 1 || a.Min < 0 || a.Min > a.Max {
		return fmt.Errorf("anway: invalid autoscale min %d and max %d", a.Min, a.Max)
	}

	return nil
}

// Worker fetches and runs jobs.
type Worker struct {
	url           string
	options       WorkerOptions
	handlers      map[string]Handler
	middleware    []Middleware
	subscriptions []Subscription

	// slots counts the slots that run now, in all Run calls.
	slots atomic.Int64
}

// NewWorker creates a worker. A blank URL uses ANWAY_URL. The worker fetches
// from Queues and from the queues that On adds. Without any queue, it fetches
// from "default".
func NewWorker(serverURL string, options WorkerOptions) *Worker {
	options.Queues = slices.Clone(options.Queues)

	if options.Concurrency <= 0 {
		options.Concurrency = defaultConcurrency
	}

	if options.MaxConcurrency > 0 {
		options.Concurrency = min(options.Concurrency, options.MaxConcurrency)
	}

	if options.Autoscale != nil {
		scale := *options.Autoscale
		options.Autoscale = &scale
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

// On declares the handler of an event. The worker runs jobs whose job type
// is the event. The option sets merge in order, as Options describes.
//
// A declaration uses only Queue, Retry, ReserveFor, and Topics. For each
// topic, the worker records one subscription with the event as the job type
// and the merged queue, retry, and reserve_for, and Run sends SUBSCRIBE for it
// before its first FETCH. Each start sends them again, so the code of the
// worker sets them, as in the Ruby and JS clients. When the declaration has
// topics or sets a queue, the worker also fetches from that queue. At,
// Backtrace, UniqueFor, UniqueUntil, ExpiresIn, ExpiresAt, Track, OnDead, and
// Custom are push keys, and they have no effect here.
//
// A worker has one handler for each event. On panics when the event already
// has a handler. To send one topic to several flows, declare several events,
// each with the topic.
func (w *Worker) On(event string, handler Handler, options ...Options) {
	if _, taken := w.handlers[event]; taken {
		panic(fmt.Sprintf("anway: multiple handlers for event %q", event))
	}

	declaration, topics := translate(event, options)

	w.handlers[event] = handler

	for _, topic := range topics {
		w.subscriptions = append(w.subscriptions, Subscription{
			Topic:      topic,
			Queue:      declaration.Queue,
			Event:      event,
			Retry:      declaration.Retry,
			ReserveFor: declaration.ReserveFor,
		})
	}

	fetches := len(topics) > 0 || slices.ContainsFunc(options, func(set Options) bool { return set.Queue != "" })

	if fetches && !slices.Contains(w.options.Queues, declaration.Queue) {
		w.options.Queues = append(w.options.Queues, declaration.Queue)
	}
}

// Use adds middleware. The first one added is the outermost.
func (w *Worker) Use(middleware ...Middleware) {
	w.middleware = append(w.middleware, middleware...)
}

// session is the state of one Run call. Each slot is a goroutine that
// fetches and runs one job at a time. concurrency is the slot count that the
// worker must run, and retiring is the count of slots that must stop after
// their job. The running slots are concurrency plus retiring.
type session struct {
	client     *Client
	queues     []string
	jobCtx     context.Context
	cancelJobs context.CancelFunc
	stop       chan struct{}
	stopOnce   sync.Once
	finished   chan struct{}
	slots      sync.WaitGroup

	// mu guards running, concurrency, and retiring.
	mu          sync.Mutex
	running     map[string]*Job
	concurrency int
	retiring    int
}

func (s *session) threads() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.concurrency
}

// retire tells a slot to stop when a stop is pending, and takes that stop.
func (s *session) retire() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.retiring == 0 {
		return false
	}

	s.retiring--

	return true
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
//
// The first BEAT goes out at start, with the thread count, so the server can
// scale the worker at once. When a BEAT reply has a new count, the worker
// starts goroutines at once, or asks goroutines to stop after their job, and
// then reports the new count at once.
func (w *Worker) Run(ctx context.Context) error {
	if err := w.options.Autoscale.check(); err != nil {
		return err
	}

	wid, err := randomID()
	if err != nil {
		return err
	}

	queues := w.options.Queues

	if len(queues) == 0 {
		queues = []string{defaultQueue}
	}

	options := w.options.Client
	options.WID = wid
	options.Labels = append([]string{"go"}, w.options.Labels...)
	options.PoolSize = w.options.Concurrency + 1

	options.identity = &identity{
		Hostname:       w.options.Hostname,
		Tag:            w.options.Tag,
		Queues:         queues,
		Concurrency:    w.options.Concurrency,
		Autoscale:      w.options.Autoscale,
		MaxConcurrency: w.options.MaxConcurrency,
	}

	client, err := openClient(w.url, options, w.limit()+1)
	if err != nil {
		return err
	}
	defer client.Close()

	first, err := client.Heartbeat(ctx, "", w.options.Concurrency)
	if err != nil && ctx.Err() == nil {
		return err
	}

	if err := w.subscribe(ctx, client); err != nil && ctx.Err() == nil {
		return err
	}

	jobCtx, cancelJobs := context.WithCancel(context.Background())
	defer cancelJobs()

	current := &session{
		client:      client,
		queues:      queues,
		jobCtx:      jobCtx,
		cancelJobs:  cancelJobs,
		stop:        make(chan struct{}),
		finished:    make(chan struct{}),
		running:     map[string]*Job{},
		concurrency: w.options.Concurrency,
	}

	for range w.options.Concurrency {
		w.startSlot(current)
	}

	go func() {
		current.slots.Wait()
		close(current.finished)
	}()

	state, terminate := w.obey(ctx, current, first, "")

	ticker := time.NewTicker(w.options.BeatInterval)
	defer ticker.Stop()

	for !terminate {
		select {
		case <-ctx.Done():
			terminate = true
		case <-ticker.C:
			state, terminate = w.beat(ctx, current, state)
		}
	}

	w.shutdown(current)

	return nil
}

// limit is the most slots that the worker can run.
func (w *Worker) limit() int {
	if w.options.MaxConcurrency > 0 {
		return w.options.MaxConcurrency
	}

	return max(maxThreads, w.options.Concurrency)
}

// beat sends a BEAT with the threads that run now, and obeys the reply. It
// returns the new state, and true at terminate.
func (w *Worker) beat(ctx context.Context, current *session, state string) (string, bool) {
	reply, err := current.client.Heartbeat(ctx, state, current.threads())
	if err != nil {
		w.options.Logger.Warn("beat failed", "error", err)

		return state, false
	}

	return w.obey(ctx, current, reply, state)
}

// obey applies a BEAT reply, which can have a state, a thread count, or both.
// After a change of the threads, the worker sends a BEAT with the new count
// at once, not at the next interval, so the server shows the threads that
// run, and the next autoscale step starts from them.
func (w *Worker) obey(ctx context.Context, current *session, reply Heartbeat, state string) (string, bool) {
	if reply.State == "terminate" {
		return state, true
	}

	resized := reply.Concurrency > 0 && w.resize(current, reply.Concurrency)

	if reply.State == "quiet" {
		state = reply.State
		current.stopFetching()
	}

	if resized {
		return w.beat(ctx, current, state)
	}

	return state, false
}

// resize starts slots at once, or asks slots to stop after their job. A stop
// that is still pending is canceled first, so a slot does not stop and then
// start again. The count never goes above the limit of the worker, and the
// connection pool grows with it. It returns true when the count changed.
//
// Only the beat loop calls resize, and the beat loop also stops the session.
// A stopped session does not resize, so a slot never starts after a stop.
func (w *Worker) resize(current *session, size int) bool {
	if current.stopped() {
		return false
	}

	size = min(size, w.limit())

	current.mu.Lock()

	change := size - current.concurrency

	if change > 0 {
		kept := min(current.retiring, change)
		current.retiring -= kept

		current.client.grow(size + 1)

		for range change - kept {
			w.startSlot(current)
		}
	}

	if change < 0 {
		current.retiring -= change
	}

	current.concurrency = size

	current.mu.Unlock()

	if change == 0 {
		return false
	}

	w.options.Logger.Info("threads changed", "concurrency", size)

	return true
}

// startSlot starts one slot. The running slots never fall to zero before the
// session stops, so this Add never races with the Wait of the session.
func (w *Worker) startSlot(current *session) {
	w.slots.Add(1)

	current.slots.Go(func() {
		defer w.slots.Add(-1)

		w.fetchLoop(current)
	})
}

// subscribe sends SUBSCRIBE for each subscription of the worker. The server
// keeps one subscription for the same topic, queue, and event.
func (w *Worker) subscribe(ctx context.Context, client *Client) error {
	for _, subscription := range w.subscriptions {
		if err := client.Subscribe(ctx, subscription); err != nil {
			return fmt.Errorf("subscribe %s to %s: %w", subscription.Event, subscription.Topic, err)
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

// fetchLoop is one slot. It never cancels a fetch in progress. A canceled
// fetch can lose its reply, and the job then waits for its reservation to
// expire. A slot that must stop does so only between jobs, so its job always
// finishes and is reported.
func (w *Worker) fetchLoop(current *session) {
	for !current.stopped() && !current.retire() {
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
