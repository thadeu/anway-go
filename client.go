// Package anway is the Go client and worker for the Anway job server.
package anway

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultURL     = "tcp://127.0.0.1:7419"
	defaultPool    = 8
	defaultTimeout = 10 * time.Second
	fetchWait      = 2 * time.Second
)

// ClientOptions changes how a client connects. All fields are optional.
type ClientOptions struct {
	PoolSize int
	Timeout  time.Duration
	TLS      *tls.Config
	WID      string
	Labels   []string

	identity *identity
}

// identity holds the HELLO fields that only a worker sends. A zero field is
// not sent. A blank Hostname is the machine name.
type identity struct {
	Hostname       string
	Tag            string
	Queues         []string
	Concurrency    int
	Autoscale      *Autoscale
	MaxConcurrency int
}

type endpoint struct {
	address  string
	password string
	tls      bool
}

// MaxBulk is the most jobs that one PushBulk can send.
const MaxBulk = 1000

// Client talks to the server. It is safe for concurrent use.
type Client struct {
	target  endpoint
	options ClientOptions
	slots   chan *conn
	closed  chan struct{}
	once    sync.Once

	mu   sync.Mutex
	size int
}

// Open creates a client. A blank URL uses ANWAY_URL, then the local default.
// The connections open at first use.
func Open(serverURL string, options ClientOptions) (*Client, error) {
	return openClient(serverURL, options, 0)
}

// openClient creates a client whose pool can grow to capacity connections.
// A capacity below the pool size is the pool size.
func openClient(serverURL string, options ClientOptions, capacity int) (*Client, error) {
	if serverURL == "" {
		serverURL = os.Getenv("ANWAY_URL")
	}

	if serverURL == "" {
		serverURL = defaultURL
	}

	target, err := parseURL(serverURL)
	if err != nil {
		return nil, err
	}

	if options.PoolSize <= 0 {
		options.PoolSize = defaultPool
	}

	if options.Timeout <= 0 {
		options.Timeout = defaultTimeout
	}

	client := &Client{
		target:  target,
		options: options,
		slots:   make(chan *conn, max(capacity, options.PoolSize)),
		closed:  make(chan struct{}),
	}

	client.grow(options.PoolSize)

	return client, nil
}

// grow lets the pool hold size connections at once, up to its capacity. The
// pool does not shrink. An idle connection stays open until Close.
func (c *Client) grow(size int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	select {
	case <-c.closed:
		return
	default:
	}

	for c.size < min(size, cap(c.slots)) {
		c.slots <- nil
		c.size++
	}
}

func parseURL(raw string) (endpoint, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return endpoint{}, fmt.Errorf("anway: invalid URL: %w", err)
	}

	if parsed.Scheme != "tcp" && parsed.Scheme != "tcp+tls" {
		return endpoint{}, fmt.Errorf("anway: unsupported scheme %q", parsed.Scheme)
	}

	password, _ := parsed.User.Password()

	return endpoint{address: parsed.Host, password: password, tls: parsed.Scheme == "tcp+tls"}, nil
}

// Close closes all idle connections and waits for connections in use.
func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.closed)

		c.mu.Lock()
		size := c.size
		c.mu.Unlock()

		for range size {
			if current := <-c.slots; current != nil {
				current.close()
			}
		}
	})

	return nil
}

// call runs one command on a pooled connection. A broken connection closes,
// and the next call opens a new one.
func (c *Client) call(ctx context.Context, verb string, argument any, wait time.Duration) (string, error) {
	var current *conn

	select {
	case <-c.closed:
		return "", ErrClosed
	default:
	}

	select {
	case current = <-c.slots:
	case <-c.closed:
		return "", ErrClosed
	case <-ctx.Done():
		return "", ctx.Err()
	}

	if current == nil {
		opened, err := dial(ctx, c.target, c.options)
		if err != nil {
			c.slots <- nil

			return "", err
		}

		current = opened
	}

	current.raw.SetDeadline(time.Now().Add(c.options.Timeout + wait))

	stop := context.AfterFunc(ctx, func() { current.raw.SetDeadline(time.Now()) })
	reply, err := current.command(verb, argument)
	stop()

	var broken *transportError

	if errors.As(err, &broken) {
		current.close()
		c.slots <- nil

		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		return "", err
	}

	c.slots <- current

	return reply, err
}

func (c *Client) callJSON(ctx context.Context, verb string, argument, target any) error {
	reply, err := c.call(ctx, verb, argument, 0)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(reply), target)
}

// Push sends a job. It returns ErrNotUnique when a unique lock blocks it.
func (c *Client) Push(ctx context.Context, job *Job) error {
	_, err := c.call(ctx, "PUSH", job, 0)

	return err
}

// PushBulk sends up to MaxBulk jobs with one PUSHB, and the server saves
// them in one write. It returns the reason for each job that is not in, by
// jid, such as a duplicate jid or a unique lock. The map is empty when all
// jobs are in. A refused job does not stop the others. More than MaxBulk
// jobs is an error, and nothing goes to the server.
func (c *Client) PushBulk(ctx context.Context, jobs []*Job) (map[string]string, error) {
	if len(jobs) > MaxBulk {
		return nil, fmt.Errorf("anway: a bulk push has at most %d jobs, not %d", MaxBulk, len(jobs))
	}

	failures := map[string]string{}

	if len(jobs) == 0 {
		return failures, nil
	}

	return failures, c.callJSON(ctx, "PUSHB", jobs, &failures)
}

// Fetch returns the next job, or nil when no job arrives in about 2 seconds.
// The client needs a WID.
func (c *Client) Fetch(ctx context.Context, queues ...string) (*Job, error) {
	reply, err := c.call(ctx, "FETCH", strings.Join(queues, " "), fetchWait)
	if err != nil || reply == "" {
		return nil, err
	}

	job := &Job{}

	if err := json.Unmarshal([]byte(reply), job); err != nil {
		return nil, err
	}

	return job, nil
}

// Ack reports success.
func (c *Client) Ack(ctx context.Context, jid string) error {
	_, err := c.call(ctx, "ACK", map[string]string{"jid": jid}, 0)

	return err
}

// Fail reports a failure with an error type and message.
func (c *Client) Fail(ctx context.Context, jid, errType, message string, backtrace []string) error {
	report := map[string]any{"jid": jid, "errtype": errType, "message": message, "backtrace": backtrace}
	_, err := c.call(ctx, "FAIL", report, 0)

	return err
}

// Beat reports liveness. It returns "", "quiet", or "terminate".
func (c *Client) Beat(ctx context.Context, currentState string) (string, error) {
	reply, err := c.Heartbeat(ctx, currentState, 0)

	return reply.State, err
}

// Heartbeat is the reply to a BEAT. State is "", "quiet", or "terminate".
// Concurrency is the thread count that the server wants, or 0 for no change.
// A reply can have both.
type Heartbeat struct {
	State       string `json:"state"`
	Concurrency int    `json:"concurrency"`
}

// Heartbeat reports liveness and the threads that the worker runs now. A
// concurrency of 0 does not send the count, and the server then never asks
// for a change.
func (c *Client) Heartbeat(ctx context.Context, currentState string, concurrency int) (Heartbeat, error) {
	request := map[string]any{"wid": c.options.WID}

	if currentState != "" {
		request["current_state"] = currentState
	}

	if concurrency > 0 {
		request["concurrency"] = concurrency
	}

	var decoded Heartbeat

	reply, err := c.call(ctx, "BEAT", request, 0)
	if err != nil || reply == "OK" {
		return decoded, err
	}

	return decoded, json.Unmarshal([]byte(reply), &decoded)
}

// Info returns server data.
func (c *Client) Info(ctx context.Context) (map[string]any, error) {
	info := map[string]any{}

	return info, c.callJSON(ctx, "INFO", nil, &info)
}

// QueueSizes returns the waiting jobs of each queue.
func (c *Client) QueueSizes(ctx context.Context) (map[string]uint64, error) {
	var info struct {
		Stats struct {
			Queues map[string]uint64 `json:"queues"`
		} `json:"faktory"`
	}

	return info.Stats.Queues, c.callJSON(ctx, "INFO", nil, &info)
}

// PauseQueues stops fetch on the queues. "*" means all queues.
func (c *Client) PauseQueues(ctx context.Context, queues ...string) error {
	return c.queue(ctx, "PAUSE", queues)
}

// ResumeQueues allows fetch on the queues.
func (c *Client) ResumeQueues(ctx context.Context, queues ...string) error {
	return c.queue(ctx, "RESUME", queues)
}

// RemoveQueues deletes the queues and their waiting jobs.
func (c *Client) RemoveQueues(ctx context.Context, queues ...string) error {
	return c.queue(ctx, "REMOVE", queues)
}

func (c *Client) queue(ctx context.Context, action string, queues []string) error {
	_, err := c.call(ctx, "QUEUE", action+" "+strings.Join(queues, " "), 0)

	return err
}

// Filter selects jobs for Mutate. Blank fields match all jobs.
type Filter struct {
	Event  string   `json:"jobtype,omitempty"`
	JIDs   []string `json:"jids,omitempty"`
	Regexp string   `json:"regexp,omitempty"`
}

// Mutate changes jobs in "retries", "scheduled", or "dead".
// The command is "kill", "discard", "requeue", or "clear".
func (c *Client) Mutate(ctx context.Context, command, target string, filter Filter) error {
	request := map[string]any{"cmd": command, "target": target, "filter": filter}
	_, err := c.call(ctx, "MUTATE", request, 0)

	return err
}

// Batch defines a batch. It needs a Success or a Complete callback.
type Batch struct {
	ParentBID   string `json:"parent_bid,omitempty"`
	Description string `json:"description,omitempty"`
	Success     *Job   `json:"success,omitempty"`
	Complete    *Job   `json:"complete,omitempty"`
}

// BatchStatus is the state of a batch.
type BatchStatus struct {
	BID           string `json:"bid"`
	ParentBID     string `json:"parent_bid,omitempty"`
	Description   string `json:"description,omitempty"`
	CreatedAt     string `json:"created_at"`
	CompleteState string `json:"complete_st"`
	SuccessState  string `json:"success_st"`
	Total         int64  `json:"total"`
	Pending       int64  `json:"pending"`
	Failed        int64  `json:"failed"`
}

// BatchNew creates an open batch and returns its id.
func (c *Client) BatchNew(ctx context.Context, batch Batch) (string, error) {
	encoded, err := json.Marshal(batch)
	if err != nil {
		return "", err
	}

	return c.call(ctx, "BATCH", "NEW "+string(encoded), 0)
}

// BatchOpen allows new jobs in a committed batch.
func (c *Client) BatchOpen(ctx context.Context, bid string) error {
	_, err := c.call(ctx, "BATCH", "OPEN "+bid, 0)

	return err
}

// BatchCommit closes the batch definition.
func (c *Client) BatchCommit(ctx context.Context, bid string) error {
	_, err := c.call(ctx, "BATCH", "COMMIT "+bid, 0)

	return err
}

// BatchStatus returns the state of a batch.
func (c *Client) BatchStatus(ctx context.Context, bid string) (BatchStatus, error) {
	var status BatchStatus

	return status, c.callJSON(ctx, "BATCH", "STATUS "+bid, &status)
}

// Track is the progress of a job.
type Track struct {
	JID       string `json:"jid"`
	State     string `json:"state"`
	UpdatedAt string `json:"updated_at,omitempty"`
	Percent   *int   `json:"percent,omitempty"`
	Desc      string `json:"desc,omitempty"`
}

// TrackUpdate changes the progress of a job.
type TrackUpdate struct {
	JID          string `json:"jid"`
	Percent      *int   `json:"percent,omitempty"`
	Desc         string `json:"desc,omitempty"`
	ReserveUntil string `json:"reserve_until,omitempty"`
}

// TrackGet returns the progress of a job.
func (c *Client) TrackGet(ctx context.Context, jid string) (Track, error) {
	var track Track

	return track, c.callJSON(ctx, "TRACK", "GET "+jid, &track)
}

// TrackSet updates the progress of a job.
func (c *Client) TrackSet(ctx context.Context, update TrackUpdate) error {
	encoded, err := json.Marshal(update)
	if err != nil {
		return err
	}

	_, err = c.call(ctx, "TRACK", "SET "+string(encoded), 0)

	return err
}
