package anway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testServer struct {
	addr string
	dir  string
	cmd  *exec.Cmd
	env  []string
}

func serverBinary(t *testing.T) string {
	t.Helper()

	path := os.Getenv("SERVER_BIN")
	if path == "" {
		t.Skip("SERVER_BIN is not set")
	}

	return path
}

func freeAddr(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	return listener.Addr().String()
}

func startServer(t *testing.T, env ...string) *testServer {
	t.Helper()

	server := &testServer{addr: freeAddr(t), dir: t.TempDir(), env: env}

	server.start(t)

	t.Cleanup(func() { server.stop() })

	return server
}

func (s *testServer) start(t *testing.T) {
	t.Helper()

	s.cmd = exec.Command(serverBinary(t))
	s.cmd.Env = append(os.Environ(), "BIND_ADDR="+s.addr, "DATA_DIR="+s.dir, "SHUTDOWN_TIMEOUT=1s")
	s.cmd.Env = append(s.cmd.Env, s.env...)

	if err := s.cmd.Start(); err != nil {
		t.Fatal(err)
	}

	for range 200 {
		conn, err := net.Dial("tcp", s.addr)
		if err == nil {
			conn.Close()

			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("server did not start")
}

func (s *testServer) stop() {
	s.cmd.Process.Kill()
	s.cmd.Wait()
}

func (s *testServer) url() string {
	return "tcp://" + s.addr
}

func open(t *testing.T, serverURL string) *Client {
	t.Helper()

	client, err := Open(serverURL, ClientOptions{WID: fmt.Sprintf("test-%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { client.Close() })

	return client
}

func newJob(t *testing.T, event string, args []any, options ...Options) *Job {
	t.Helper()

	job, err := NewJob(event, args, options...)
	if err != nil {
		t.Fatal(err)
	}

	return job
}

func TestC01ConnectWithURLAndEnv(t *testing.T) {
	server := startServer(t)

	t.Setenv("ANWAY_URL", server.url())

	if _, err := open(t, "").Info(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestC02Password(t *testing.T) {
	server := startServer(t, "SERVER_PASSWORD=secret")

	if _, err := open(t, "tcp://:secret@"+server.addr).Info(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := open(t, "tcp://:wrong@"+server.addr).Info(context.Background()); err == nil {
		t.Fatal("a wrong password must fail")
	}
}

func TestC03TLS(t *testing.T) {
	serverBinary(t)

	source := httptest.NewUnstartedServer(nil)
	source.StartTLS()

	certificate := source.TLS.Certificates[0]
	source.Close()

	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")
	key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)

	if err != nil {
		t.Fatal(err)
	}

	writePEM(t, certFile, "CERTIFICATE", certificate.Certificate[0])
	writePEM(t, keyFile, "PRIVATE KEY", key)

	server := startServer(t, "TLS_CERT_FILE="+certFile, "TLS_KEY_FILE="+keyFile)
	roots := x509.NewCertPool()
	parsed, err := x509.ParseCertificate(certificate.Certificate[0])

	if err != nil {
		t.Fatal(err)
	}

	roots.AddCert(parsed)

	options := ClientOptions{TLS: &tls.Config{RootCAs: roots, ServerName: "example.com"}}

	client, err := Open("tcp+tls://"+server.addr, options)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Info(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func writePEM(t *testing.T, path, kind string, raw []byte) {
	t.Helper()

	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: raw}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestC04PushFetchAck(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "email", []any{"a@b.com", 42})); err != nil {
		t.Fatal(err)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	var (
		address string
		count   int
	)

	if err := job.Bind(&address, &count); err != nil {
		t.Fatal(err)
	}

	if job.Type != "email" || address != "a@b.com" || count != 42 {
		t.Fatalf("unexpected job: %+v", job)
	}

	if err := client.Ack(ctx, job.JID); err != nil {
		t.Fatal(err)
	}
}

func TestC05EmptyFetch(t *testing.T) {
	job, err := open(t, startServer(t).url()).Fetch(context.Background(), "default")
	if job != nil || err != nil {
		t.Fatalf("unexpected result: %+v %v", job, err)
	}
}

func TestC06Fail(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()
	pushed := newJob(t, "x", nil)

	if err := client.Push(ctx, pushed); err != nil {
		t.Fatal(err)
	}

	if _, err := client.Fetch(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	if err := client.Fail(ctx, pushed.JID, "Boom", "failed", nil); err != nil {
		t.Fatal(err)
	}

	if err := client.Mutate(ctx, "requeue", "retries", Filter{}); err != nil {
		t.Fatal(err)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	if job.Failure == nil || job.Failure.ErrType != "Boom" || job.Failure.Message != "failed" {
		t.Fatalf("unexpected failure: %+v", job.Failure)
	}
}

func TestC07NotUnique(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x", []any{1}, Options{UniqueFor: time.Minute})); err != nil {
		t.Fatal(err)
	}

	err := client.Push(ctx, newJob(t, "x", []any{1}, Options{UniqueFor: time.Minute}))
	if !errors.Is(err, ErrNotUnique) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestC08Scheduled(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x", nil, Options{At: time.Now().Add(time.Hour)})); err != nil {
		t.Fatal(err)
	}

	if job, err := client.Fetch(ctx, "default"); job != nil || err != nil {
		t.Fatalf("unexpected result: %+v %v", job, err)
	}
}

func TestC09PauseResume(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	if err := client.PauseQueues(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	if job, _ := client.Fetch(ctx, "default"); job != nil {
		t.Fatal("a paused queue must not send jobs")
	}

	if err := client.ResumeQueues(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	if job, _ := client.Fetch(ctx, "default"); job == nil {
		t.Fatal("expected a job")
	}
}

func TestC10MutateRequeue(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	for _, kind := range []string{"a", "b"} {
		pushed := newJob(t, kind, nil)

		if err := client.Push(ctx, pushed); err != nil {
			t.Fatal(err)
		}

		if _, err := client.Fetch(ctx, "default"); err != nil {
			t.Fatal(err)
		}

		if err := client.Fail(ctx, pushed.JID, "Boom", "failed", nil); err != nil {
			t.Fatal(err)
		}
	}

	if err := client.Mutate(ctx, "requeue", "retries", Filter{Event: "a"}); err != nil {
		t.Fatal(err)
	}

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}

	job, err := client.Fetch(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}

	if job == nil || job.Type != "a" {
		t.Fatalf("unexpected job: %+v", job)
	}
}

func TestC11Batch(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	bid, err := client.BatchNew(ctx, Batch{Success: newJob(t, "on-success", nil), Complete: newJob(t, "on-complete", nil)})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Push(ctx, newJob(t, "member", nil).SetCustom("bid", bid)); err != nil {
		t.Fatal(err)
	}

	if err := client.BatchCommit(ctx, bid); err != nil {
		t.Fatal(err)
	}

	for _, want := range []string{"member", "on-complete", "on-success"} {
		job, err := client.Fetch(ctx, "default")
		if err != nil {
			t.Fatal(err)
		}

		if job == nil || job.Type != want {
			t.Fatalf("unexpected job, want %s: %+v", want, job)
		}

		if err := client.Ack(ctx, job.JID); err != nil {
			t.Fatal(err)
		}
	}

	status, err := client.BatchStatus(ctx, bid)
	if err != nil {
		t.Fatal(err)
	}

	if status.Total != 1 || status.Pending != 0 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestC12Track(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()
	pushed := newJob(t, "x", nil, Options{Track: true})

	if err := client.Push(ctx, pushed); err != nil {
		t.Fatal(err)
	}

	percent := 40

	if err := client.TrackSet(ctx, TrackUpdate{JID: pushed.JID, Percent: &percent, Desc: "step"}); err != nil {
		t.Fatal(err)
	}

	track, err := client.TrackGet(ctx, pushed.JID)
	if err != nil {
		t.Fatal(err)
	}

	if track.Percent == nil || *track.Percent != 40 || track.Desc != "step" {
		t.Fatalf("unexpected track: %+v", track)
	}
}

func TestC13Info(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	sizes, err := client.QueueSizes(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if sizes["default"] != 1 {
		t.Fatalf("unexpected sizes: %v", sizes)
	}
}

func TestC14ConcurrentCallers(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	var group sync.WaitGroup

	for range 100 {
		group.Go(func() {
			if err := client.Push(ctx, newJob(t, "x", nil)); err != nil {
				t.Error(err)
			}
		})
	}

	group.Wait()

	if sizes, err := client.QueueSizes(ctx); err != nil || sizes["default"] != 100 {
		t.Fatalf("unexpected sizes: %v %v", sizes, err)
	}
}

func TestC16ClosedClient(t *testing.T) {
	client := open(t, startServer(t).url())

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}

	if err := client.Push(context.Background(), newJob(t, "x", nil)); !errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestC15Reconnect(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	server.stop()
	server.start(t)

	var err error

	for range 3 {
		if err = client.Push(ctx, newJob(t, "x", nil)); err == nil {
			break
		}
	}

	if err != nil {
		t.Fatal(err)
	}

	if sizes, err := client.QueueSizes(ctx); err != nil || sizes["default"] != 2 {
		t.Fatalf("unexpected sizes: %v %v", sizes, err)
	}
}

func TestC17PushBulk(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()
	existing := newJob(t, "x", nil)

	if err := client.Push(ctx, existing); err != nil {
		t.Fatal(err)
	}

	again := newJob(t, "x", nil)
	again.JID = existing.JID

	failures, err := client.PushBulk(ctx, []*Job{newJob(t, "x", nil), again, newJob(t, "x", nil)})
	if err != nil {
		t.Fatal(err)
	}

	if len(failures) != 1 || failures[existing.JID] == "" {
		t.Fatalf("only the existing jid must be refused, with a reason: %v", failures)
	}

	failures, err = client.PushBulk(ctx, []*Job{newJob(t, "x", nil), newJob(t, "x", nil)})
	if err != nil || failures == nil || len(failures) != 0 {
		t.Fatalf("a bulk push where all jobs go in must return no failures: %v %v", failures, err)
	}

	tooMany := make([]*Job, MaxBulk+1)

	for i := range tooMany {
		tooMany[i] = newJob(t, "x", nil)
	}

	if _, err := client.PushBulk(ctx, tooMany); err == nil || !strings.Contains(err.Error(), "at most 1000") {
		t.Fatalf("more than %d jobs must be an error: %v", MaxBulk, err)
	}

	if sizes, err := client.QueueSizes(ctx); err != nil || sizes["default"] != 5 {
		t.Fatalf("unexpected sizes: %v %v", sizes, err)
	}
}

func runWorker(t *testing.T, worker *Worker) (context.CancelFunc, <-chan error) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- worker.Run(ctx) }()

	t.Cleanup(cancel)

	return cancel, done
}

func waitFor(t *testing.T, check func() bool) {
	t.Helper()

	for range 500 {
		if check() {
			return
		}

		time.Sleep(10 * time.Millisecond)
	}

	t.Fatal("condition was not met")
}

func retries(t *testing.T, client *Client) float64 {
	t.Helper()

	return taskSize(t, client, "Retries")
}

func dead(t *testing.T, client *Client) float64 {
	t.Helper()

	return taskSize(t, client, "Dead")
}

// taskSize reads the size of a job set, such as "Retries" or "Dead".
func taskSize(t *testing.T, client *Client, name string) float64 {
	t.Helper()

	info, err := client.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	tasks := info["faktory"].(map[string]any)["tasks"].(map[string]any)

	return tasks[name].(map[string]any)["size"].(float64)
}

func processed(t *testing.T, client *Client) float64 {
	t.Helper()

	info, err := client.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	return info["faktory"].(map[string]any)["total_processed"].(float64)
}

func TestW01RunsAndAcks(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 2})

	worker.On("x", func(context.Context, *Job) error { return nil })
	runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return processed(t, client) == 1 })
}

func TestW02W03W04FailureKinds(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})

	worker.On("error", func(context.Context, *Job) error { return errors.New("failed") })
	worker.On("panic", func(context.Context, *Job) error { panic("boom") })
	worker.On("ok", func(context.Context, *Job) error { return nil })
	runWorker(t, worker)

	for _, kind := range []string{"error", "panic", "unknown", "ok"} {
		if err := client.Push(context.Background(), newJob(t, kind, nil)); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool { return retries(t, client) == 3 && processed(t, client) == 1 })
}

func TestW05MiddlewareOrder(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})
	result := make(chan string, 1)

	wrap := func(name string) Middleware {
		return func(next Handler) Handler {
			return func(ctx context.Context, job *Job) error {
				return next(context.WithValue(ctx, contextKey(name), ctxOrder(ctx)+name), job)
			}
		}
	}

	worker.Use(wrap("a"), wrap("b"))

	worker.On("x", func(ctx context.Context, _ *Job) error {
		result <- ctx.Value(contextKey("b")).(string)

		return nil
	})

	runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	if order := <-result; order != "ab" {
		t.Fatalf("unexpected order: %s", order)
	}
}

type contextKey string

func ctxOrder(ctx context.Context) string {
	value, _ := ctx.Value(contextKey("a")).(string)

	return value
}

func TestW06ServerTerminateFailsUnfinishedJobs(t *testing.T) {
	script := &scriptedServer{job: `{"jid":"slow-1","jobtype":"slow","args":[],"queue":"default"}`}
	worker := NewWorker(script.start(t), WorkerOptions{
		Concurrency:     1,
		BeatInterval:    20 * time.Millisecond,
		ShutdownTimeout: 50 * time.Millisecond,
		CancelGrace:     100 * time.Millisecond,
	})
	started := make(chan struct{})

	worker.On("slow", func(ctx context.Context, _ *Job) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})

	_, done := runWorker(t, worker)

	<-started
	script.terminate.Store(true)

	waitDone(t, done)

	failures := script.failures()

	if len(failures) != 1 || failures[0].JID != "slow-1" || !strings.Contains(failures[0].Message, "context canceled") {
		t.Fatalf("the worker must FAIL the unfinished job: %+v", failures)
	}
}

func waitDone(t *testing.T, done <-chan error) {
	t.Helper()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the worker did not stop")
	}
}

func TestW07LocalStopLetsJobsFinish(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})
	started := make(chan struct{})

	var finished atomic.Bool

	worker.On("slow", func(context.Context, *Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		finished.Store(true)

		return nil
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow", nil)); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()

	if err := <-done; err != nil {
		t.Fatal(err)
	}

	if !finished.Load() || processed(t, client) != 1 {
		t.Fatal("the job must finish and ack")
	}
}

// scriptedServer acts as a server for a worker. BEAT replies quiet for beats
// 2 to lastQuiet, and terminate after lastQuiet or after terminate is set.
// beatScript, when it is set, makes the BEAT replies in place of these rules.
// The first FETCH gives out job, when it is set. HELLO, BEAT, and FAIL
// arguments are recorded.
type scriptedServer struct {
	job        string
	lastQuiet  int64
	beatScript func(count int64) string

	beats        atomic.Int64
	quietFetches atomic.Int64
	jobSent      atomic.Bool
	terminate    atomic.Bool

	mu        sync.Mutex
	failed    []failReport
	greetings []string
	beatArgs  []beatRequest
}

// beatRequest is the argument of a BEAT command.
type beatRequest struct {
	State       string `json:"current_state"`
	Concurrency int    `json:"concurrency"`
}

// failReport is the argument of a FAIL command.
type failReport struct {
	JID     string `json:"jid"`
	ErrType string `json:"errtype"`
	Message string `json:"message"`
}

const lastQuietBeat = 20

// start accepts connections until the test ends, and returns the server URL.
func (s *scriptedServer) start(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			go s.serve(conn)
		}
	}()

	return "tcp://" + listener.Addr().String()
}

func (s *scriptedServer) serve(conn net.Conn) {
	defer conn.Close()

	io.WriteString(conn, "+HI {\"v\":2}\r\n")

	lines := bufio.NewScanner(conn)

	for lines.Scan() {
		verb, argument, _ := strings.Cut(lines.Text(), " ")

		switch verb {
		case "FETCH":
			io.WriteString(conn, s.fetchReply())
		case "HELLO":
			s.record(func() { s.greetings = append(s.greetings, argument) })
			io.WriteString(conn, "+OK\r\n")
		case "BEAT":
			var request beatRequest

			json.Unmarshal([]byte(argument), &request)
			s.record(func() { s.beatArgs = append(s.beatArgs, request) })
			io.WriteString(conn, s.beatReply(s.beats.Add(1)))
		case "FAIL":
			s.recordFail(argument)
			io.WriteString(conn, "+OK\r\n")
		default:
			io.WriteString(conn, "+OK\r\n")
		}
	}
}

func (s *scriptedServer) fetchReply() string {
	if s.job != "" && s.jobSent.CompareAndSwap(false, true) {
		return bulkReply(s.job)
	}

	if s.beats.Load() > 1 {
		s.quietFetches.Add(1)
	}

	time.Sleep(5 * time.Millisecond)

	return "$-1\r\n"
}

func (s *scriptedServer) beatReply(count int64) string {
	if s.beatScript != nil {
		return s.beatScript(count)
	}

	if s.terminate.Load() || (s.lastQuiet > 0 && count > s.lastQuiet) {
		return bulkReply(`{"state":"terminate"}`)
	}

	if count > 1 && count <= s.lastQuiet {
		return bulkReply(`{"state":"quiet"}`)
	}

	return "+OK\r\n"
}

func (s *scriptedServer) recordFail(argument string) {
	var report failReport

	json.Unmarshal([]byte(argument), &report)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.failed = append(s.failed, report)
}

func (s *scriptedServer) failures() []failReport {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.failed)
}

func (s *scriptedServer) record(change func()) {
	s.mu.Lock()
	defer s.mu.Unlock()

	change()
}

func (s *scriptedServer) hellos() []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.greetings)
}

func (s *scriptedServer) beatRequests() []beatRequest {
	s.mu.Lock()
	defer s.mu.Unlock()

	return slices.Clone(s.beatArgs)
}

func bulkReply(body string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(body), body)
}

func TestW08QuietKeepsWorkerAlive(t *testing.T) {
	script := &scriptedServer{lastQuiet: lastQuietBeat}
	worker := NewWorker(script.start(t), WorkerOptions{
		Concurrency:  1,
		BeatInterval: 50 * time.Millisecond,
	})

	_, done := runWorker(t, worker)

	waitDone(t, done)

	if count := script.beats.Load(); count <= lastQuietBeat {
		t.Fatalf("the worker stopped after %d beats, before terminate", count)
	}

	if count := script.quietFetches.Load(); count > 2 {
		t.Fatalf("a quiet worker sent %d fetches", count)
	}
}

func TestW09ShutdownTimeoutCancelsJobs(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1, ShutdownTimeout: 50 * time.Millisecond})
	started := make(chan struct{})

	worker.On("slow", func(ctx context.Context, _ *Job) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow", nil)); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()
	waitDone(t, done)

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}
}

func TestW10HandlerThatIgnoresContextCannotBlockShutdown(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{
		Concurrency:     1,
		ShutdownTimeout: 50 * time.Millisecond,
		CancelGrace:     50 * time.Millisecond,
	})
	started := make(chan struct{})
	release := make(chan struct{})
	returned := make(chan struct{})

	worker.On("stuck", func(context.Context, *Job) error {
		defer close(returned)

		close(started)
		<-release

		return nil
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "stuck", nil)); err != nil {
		t.Fatal(err)
	}

	<-started
	cancel()
	waitDone(t, done)

	close(release)
	<-returned
	time.Sleep(100 * time.Millisecond)

	if count := retries(t, client); count != 1 {
		t.Fatalf("unexpected retries: %v", count)
	}

	if count := processed(t, client); count != 0 {
		t.Fatalf("a late return must not ack: %v", count)
	}
}

func TestW11ServerRestartDoesNotStopTheWorker(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 2, BeatInterval: 50 * time.Millisecond})

	worker.On("x", func(context.Context, *Job) error { return nil })

	_, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return processed(t, client) == 1 })

	server.stop()
	server.start(t)

	fresh := open(t, server.url())

	if err := fresh.Push(context.Background(), newJob(t, "x", nil)); err != nil {
		t.Fatal(err)
	}

	waitWithin(t, 10*time.Second, func() bool { return processed(t, fresh) == 2 })

	select {
	case err := <-done:
		t.Fatalf("the worker stopped at the server restart: %v", err)
	default:
	}
}

// waitWithin is waitFor with a time limit.
func waitWithin(t *testing.T, limit time.Duration, check func() bool) {
	t.Helper()

	deadline := time.Now().Add(limit)

	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("condition was not met in %v", limit)
		}

		time.Sleep(10 * time.Millisecond)
	}
}

const webToken = "t"

// webServer is a test server with the HTTP API on.
type webServer struct {
	*testServer
	web string
}

func startWebServer(t *testing.T) *webServer {
	t.Helper()

	web := freeAddr(t)

	return &webServer{testServer: startServer(t, "WEB_ADDR="+web, "WEB_TOKEN="+webToken), web: web}
}

// workerView is a worker as the HTTP API shows it.
type workerView struct {
	WID         string `json:"wid"`
	Tag         string `json:"tag"`
	Concurrency int    `json:"concurrency"`
	Wanted      *int   `json:"wanted"`
	Scalable    bool   `json:"scalable"`
}

func (s *webServer) request(t *testing.T, method, path string, body any) []byte {
	t.Helper()

	var reader io.Reader

	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}

		reader = bytes.NewReader(encoded)
	}

	request, err := http.NewRequest(method, "http://"+s.web+path, reader)
	if err != nil {
		t.Fatal(err)
	}

	request.Header.Set("Authorization", "Bearer "+webToken)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	raw, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}

	if response.StatusCode != http.StatusOK {
		t.Fatalf("%s %s: %d %s", method, path, response.StatusCode, raw)
	}

	return raw
}

// view returns the worker with the tag, or nil before it connects.
func (s *webServer) view(t *testing.T, tag string) *workerView {
	t.Helper()

	var listed struct {
		Workers []workerView `json:"workers"`
	}

	if err := json.Unmarshal(s.request(t, http.MethodGet, "/api/v1/workers", nil), &listed); err != nil {
		t.Fatal(err)
	}

	for _, worker := range listed.Workers {
		if worker.Tag == tag {
			return &worker
		}
	}

	return nil
}

func (s *webServer) scale(t *testing.T, wid string, concurrency int) {
	t.Helper()

	s.request(t, http.MethodPatch, "/api/v1/workers/"+wid, map[string]int{"concurrency": concurrency})
}

// waitScalable waits until the worker with the tag reports its threads, and
// returns its wid.
func (s *webServer) waitScalable(t *testing.T, tag string) string {
	t.Helper()

	var wid string

	waitFor(t, func() bool {
		view := s.view(t, tag)

		if view == nil || !view.Scalable {
			return false
		}

		wid = view.WID

		return true
	})

	return wid
}

func TestW12StartsAndStopsThreadsAndAStoppingThreadFinishesItsJob(t *testing.T) {
	server := startWebServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1, Tag: "w12", BeatInterval: 50 * time.Millisecond})
	release := make(chan struct{})

	var running, finished atomic.Int64

	worker.On("slow", func(ctx context.Context, _ *Job) error {
		running.Add(1)
		defer running.Add(-1)

		select {
		case <-release:
		case <-ctx.Done():
			return ctx.Err()
		}

		finished.Add(1)

		return nil
	})

	runWorker(t, worker)

	wid := server.waitScalable(t, "w12")

	server.scale(t, wid, 3)

	waitFor(t, func() bool {
		view := server.view(t, "w12")

		return view.Concurrency == 3 && view.Wanted == nil
	})

	if count := worker.slots.Load(); count != 3 {
		t.Fatalf("the worker runs %d threads, want 3", count)
	}

	for range 3 {
		if err := client.Push(context.Background(), newJob(t, "slow", nil)); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool { return running.Load() == 3 })

	server.scale(t, wid, 1)

	waitFor(t, func() bool { return server.view(t, "w12").Concurrency == 1 })

	if count := worker.slots.Load(); count != 3 || running.Load() != 3 {
		t.Fatalf("a thread must not stop before its job ends: %d threads, %d jobs", count, running.Load())
	}

	close(release)

	waitFor(t, func() bool { return worker.slots.Load() == 1 && processed(t, client) == 3 })

	if count := finished.Load(); count != 3 || retries(t, client) != 0 {
		t.Fatalf("each stopping thread must finish its job: %d finished, %v retries", count, retries(t, client))
	}
}

func TestW12ScaleDownThenUpWhileJobsRun(t *testing.T) {
	server := startWebServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 3, Tag: "w12", BeatInterval: 50 * time.Millisecond})
	release := make(chan struct{})

	var running atomic.Int64

	worker.On("slow", func(context.Context, *Job) error {
		running.Add(1)
		<-release

		return nil
	})

	runWorker(t, worker)

	wid := server.waitScalable(t, "w12")

	for range 3 {
		if err := client.Push(context.Background(), newJob(t, "slow", nil)); err != nil {
			t.Fatal(err)
		}
	}

	waitFor(t, func() bool { return running.Load() == 3 })

	server.scale(t, wid, 1)

	waitFor(t, func() bool { return server.view(t, "w12").Concurrency == 1 })

	server.scale(t, wid, 3)

	waitFor(t, func() bool { return server.view(t, "w12").Concurrency == 3 })

	if count := worker.slots.Load(); count != 3 {
		t.Fatalf("the worker runs %d threads, want 3", count)
	}

	close(release)

	waitFor(t, func() bool { return processed(t, client) == 3 })

	time.Sleep(200 * time.Millisecond)

	if count := worker.slots.Load(); count != 3 {
		t.Fatalf("a canceled stop must not stop a thread: %d threads, want 3", count)
	}
}

// With one P, a new goroutine does not run before this test reads the
// counts, so a slot that starts and takes a pending stop at once is seen.
func TestW12MoreThreadsCancelAPendingStopFirst(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))

	worker := NewWorker("", WorkerOptions{Concurrency: 3, Logger: slog.New(slog.DiscardHandler)})

	client, err := openClient("tcp://127.0.0.1:1", ClientOptions{PoolSize: 4}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	current := &session{client: client, stop: make(chan struct{}), running: map[string]*Job{}, concurrency: 3}
	defer current.stopFetching()

	if !worker.resize(current, 1) || current.retiring != 2 {
		t.Fatalf("fewer threads must ask 2 threads to stop: %d pending", current.retiring)
	}

	if !worker.resize(current, 4) {
		t.Fatal("the count must change")
	}

	current.mu.Lock()
	retiring, concurrency := current.retiring, current.concurrency
	current.mu.Unlock()

	if started := worker.slots.Load(); started != 1 || retiring != 0 || concurrency != 4 {
		t.Fatalf("more threads must cancel the pending stops first: %d started, %d pending", started, retiring)
	}
}

func TestW12ReportsTheNewThreadCountAtOnce(t *testing.T) {
	server := startWebServer(t)
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1, Tag: "w12", BeatInterval: 2 * time.Second})

	runWorker(t, worker)

	wid := server.waitScalable(t, "w12")

	server.scale(t, wid, 3)

	waitWithin(t, 5*time.Second, func() bool { return worker.slots.Load() == 3 })
	waitWithin(t, 500*time.Millisecond, func() bool { return server.view(t, "w12").Concurrency == 3 })
}

func TestW12HelloIdentityAndAReplyWithStateAndConcurrency(t *testing.T) {
	script := &scriptedServer{beatScript: func(count int64) string {
		if count == 2 {
			return bulkReply(`{"state":"quiet","concurrency":9}`)
		}

		return "+OK\r\n"
	}}

	worker := NewWorker(script.start(t), WorkerOptions{
		Queues:         []string{"critical", "default"},
		Concurrency:    2,
		MaxConcurrency: 6,
		Autoscale:      &Autoscale{Max: 8},
		Hostname:       "worker.1",
		Tag:            "shop",
		BeatInterval:   50 * time.Millisecond,
	})

	runWorker(t, worker)

	waitFor(t, func() bool { return len(script.beatRequests()) >= 3 })

	var greeting map[string]any

	if err := json.Unmarshal([]byte(script.hellos()[0]), &greeting); err != nil {
		t.Fatal(err)
	}

	identity := map[string]any{
		"hostname":        "worker.1",
		"tag":             "shop",
		"concurrency":     2.0,
		"queues":          []any{"critical", "default"},
		"autoscale":       map[string]any{"max": 8.0},
		"max_concurrency": 6.0,
		"pid":             float64(os.Getpid()),
	}

	for key, want := range identity {
		if !reflect.DeepEqual(greeting[key], want) {
			t.Fatalf("HELLO %s is %v, want %v", key, greeting[key], want)
		}
	}

	beats := script.beatRequests()

	if beats[0].Concurrency != 2 || beats[1].Concurrency != 2 {
		t.Fatalf("each BEAT must send the threads, also the first: %+v", beats)
	}

	if beats[2].Concurrency != 6 || beats[2].State != "quiet" {
		t.Fatalf("the worker must obey state and concurrency, up to its limit: %+v", beats[2])
	}

	time.Sleep(100 * time.Millisecond)

	if count := script.quietFetches.Load(); count > 6 {
		t.Fatalf("a quiet worker sent %d fetches", count)
	}
}

const chatTopic = "chat.message"

func subscribe(t *testing.T, client *Client, queue, event string) {
	t.Helper()

	subscription := Subscription{Topic: chatTopic, Queue: queue, Event: event}

	if err := client.Subscribe(context.Background(), subscription); err != nil {
		t.Fatal(err)
	}
}

func publish(t *testing.T, client *Client, options PublishOptions, args ...any) Published {
	t.Helper()

	published, err := client.Publish(context.Background(), chatTopic, args, options)
	if err != nil {
		t.Fatal(err)
	}

	return published
}

// waitSubscribed publishes probe events until one makes the given number of
// jobs. Handlers see the probes, and ignore them.
func waitSubscribed(t *testing.T, client *Client, jobs int) {
	t.Helper()

	waitFor(t, func() bool { return publish(t, client, PublishOptions{}).Jobs == jobs })
}

// eventLog records the events that a handler runs.
type eventLog struct {
	mu     sync.Mutex
	counts map[string]int
}

func (l *eventLog) handler(prefix string) Handler {
	return func(_ context.Context, job *Job) error {
		event, _ := job.Custom["event"].(string)

		if !strings.HasPrefix(event, prefix) {
			return nil
		}

		l.mu.Lock()
		defer l.mu.Unlock()

		l.counts[event]++

		return nil
	}
}

func (l *eventLog) snapshot() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()

	return maps.Clone(l.counts)
}

func TestT01PublishMakesOneJobForEachSubscription(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	subscribers := map[string]string{"realtime": "chat.broadcast", "cassandra": "chat.store", "push": "chat.notify"}

	for queue, event := range subscribers {
		subscribe(t, client, queue, event)
	}

	options := PublishOptions{ID: "msg-42", Custom: map[string]any{"tenant": "acme"}}
	published := publish(t, client, options, map[string]any{"room": "general", "body": "hi"})

	if published != (Published{Event: "msg-42", Jobs: 3}) {
		t.Fatalf("unexpected result: %+v", published)
	}

	for queue, event := range subscribers {
		job, err := client.Fetch(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}

		if job == nil || job.Type != event || job.Queue != queue {
			t.Fatalf("unexpected job in %s: %+v", queue, job)
		}

		if job.Custom["topic"] != chatTopic || job.Custom["event"] != "msg-42" || job.Custom["tenant"] != "acme" {
			t.Fatalf("unexpected custom in %s: %v", queue, job.Custom)
		}

		var message map[string]string

		if err := job.Bind(&message); err != nil {
			t.Fatal(err)
		}

		if message["body"] != "hi" {
			t.Fatalf("unexpected args in %s: %s", queue, job.Args)
		}
	}
}

func TestT02SubscribeIsIdempotent(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	subscribe(t, client, "push", "chat.notify")
	subscribe(t, client, "push", "chat.notify")

	if published := publish(t, client, PublishOptions{}); published.Jobs != 1 {
		t.Fatalf("two subscribes must keep one subscription: %+v", published)
	}

	for range 2 {
		if err := client.Unsubscribe(ctx, chatTopic, "push", "chat.notify"); err != nil {
			t.Fatal(err)
		}
	}

	if published := publish(t, client, PublishOptions{}); published.Jobs != 0 {
		t.Fatalf("unsubscribe must remove the subscription: %+v", published)
	}
}

func TestT03DuplicateEvent(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	subscribe(t, client, "push", "chat.notify")

	first := publish(t, client, PublishOptions{ID: "msg-1"})
	second := publish(t, client, PublishOptions{ID: "msg-1"})

	if first != (Published{Event: "msg-1", Jobs: 1}) || second != (Published{Event: "msg-1", Duplicate: true}) {
		t.Fatalf("a second publish of msg-1 must be a duplicate: %+v %+v", first, second)
	}

	noDedup := time.Duration(0)

	for range 2 {
		if published := publish(t, client, PublishOptions{ID: "msg-2", UniqueFor: &noDedup}); published.Jobs != 1 {
			t.Fatalf("a zero UniqueFor must turn the check off: %+v", published)
		}
	}

	if sizes, err := client.QueueSizes(ctx); err != nil || sizes["push"] != 3 {
		t.Fatalf("unexpected sizes: %v %v", sizes, err)
	}
}

func TestT04EventWithNoSubscriptionGoesToDead(t *testing.T) {
	client := open(t, startServer(t).url())

	published := publish(t, client, PublishOptions{}, "hi")

	if published.Jobs != 0 || published.Duplicate || !strings.HasPrefix(published.Event, "evt-") {
		t.Fatalf("unexpected result: %+v", published)
	}

	if count := dead(t, client); count != 1 {
		t.Fatalf("unexpected dead: %v", count)
	}
}

func TestT05WorkerSubscribesWhenItStarts(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})
	log := &eventLog{counts: map[string]int{}}

	worker.On("chat.broadcast", log.handler("msg-"), Options{Queue: "realtime", Topics: []string{chatTopic}})
	runWorker(t, worker)

	waitSubscribed(t, client, 1)
	publish(t, client, PublishOptions{ID: "msg-1"}, "hi")

	waitFor(t, func() bool { return log.snapshot()["msg-1"] == 1 })
}

func TestT06TwoWorkersRunOneCopyOfEachEvent(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	logs := map[string]*eventLog{}

	for queue, event := range map[string]string{"cassandra": "chat.store", "push": "chat.notify"} {
		worker := NewWorker(server.url(), WorkerOptions{Concurrency: 2})
		logs[queue] = &eventLog{counts: map[string]int{}}

		worker.On(event, logs[queue].handler("msg-"), Options{Queue: queue, Topics: []string{chatTopic}})
		runWorker(t, worker)
	}

	waitSubscribed(t, client, 2)

	events := []string{"msg-1", "msg-2", "msg-3"}

	for _, event := range events {
		publish(t, client, PublishOptions{ID: event}, event)
	}

	for queue, log := range logs {
		waitFor(t, func() bool { return len(log.snapshot()) == len(events) })

		for event, count := range log.snapshot() {
			if !slices.Contains(events, event) || count != 1 {
				t.Fatalf("%s ran %s %d times", queue, event, count)
			}
		}
	}
}

func TestI01ContractKeysGoToWireFields(t *testing.T) {
	expiresAt := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	runAt := time.Now().Add(time.Hour)

	options := Options{
		Queue:       "critical",
		Retry:       Int(3),
		ReserveFor:  2 * time.Minute,
		Backtrace:   5,
		At:          runAt,
		UniqueFor:   time.Minute,
		UniqueUntil: "success",
		ExpiresIn:   time.Hour,
		ExpiresAt:   expiresAt,
		Track:       true,
		OnDead:      &DeadHook{Event: "email.dead", Queue: "dead-letters"},
		Custom:      map[string]any{"tenant": "acme", "track": 0, "unique_until": "start"},
	}

	job := newJob(t, "email.send", []any{"a@b.com"}, options)

	encoded, err := json.Marshal(job)
	if err != nil {
		t.Fatal(err)
	}

	var wire map[string]any

	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}

	fields := map[string]any{
		"jobtype":     "email.send",
		"queue":       "critical",
		"retry":       3.0,
		"reserve_for": 120.0,
		"backtrace":   5.0,
		"at":          runAt.UTC().Format(time.RFC3339Nano),
	}

	for key, want := range fields {
		if wire[key] != want {
			t.Fatalf("wire field %s is %v, want %v", key, wire[key], want)
		}
	}

	custom := map[string]any{
		"unique_for":   60.0,
		"unique_until": "success",
		"expires_in":   3600.0,
		"expires_at":   "2030-01-02T03:04:05Z",
		"track":        1.0,
		"on_dead":      map[string]any{"jobtype": "email.dead", "queue": "dead-letters"},
		"tenant":       "acme",
	}

	if !reflect.DeepEqual(wire["custom"], custom) {
		t.Fatalf("unexpected custom: %v", wire["custom"])
	}

	client := open(t, startServer(t).url())
	ctx := context.Background()
	pushed := newJob(t, "email.send", []any{"a@b.com"}, options, Options{At: time.Now().Add(-time.Second)})

	if err := client.Push(ctx, pushed); err != nil {
		t.Fatal(err)
	}

	fetched, err := client.Fetch(ctx, "critical")
	if err != nil {
		t.Fatal(err)
	}

	if fetched == nil || fetched.Type != "email.send" {
		t.Fatalf("unexpected job: %+v", fetched)
	}

	hook, _ := fetched.Custom["on_dead"].(map[string]any)

	if hook["jobtype"] != "email.dead" || fetched.Custom["track"] != 1.0 || fetched.Custom["unique_for"] != 60.0 {
		t.Fatalf("the server must keep the contract keys: %v", fetched.Custom)
	}
}

func TestI02OptionSetsMergeInOrder(t *testing.T) {
	first := Options{
		Queue:      "low",
		Retry:      Int(5),
		ReserveFor: 2 * time.Minute,
		Custom:     map[string]any{"a": 1, "b": 1},
		Topics:     []string{"t1", "t2"},
	}

	second := Options{
		Queue:  "high",
		Retry:  Int(0),
		Custom: map[string]any{"b": 2, "c": 2},
		Topics: []string{"t2", "t3"},
	}

	job := newJob(t, "x", nil, first, second)

	if job.Queue != "high" || job.Retry == nil || *job.Retry != 0 || job.ReserveFor != 120 {
		t.Fatalf("a later single value must win, and an unset one must not: %+v", job)
	}

	if !reflect.DeepEqual(job.Custom, map[string]any{"a": 1, "b": 2, "c": 2}) {
		t.Fatalf("unexpected custom: %v", job.Custom)
	}

	if !reflect.DeepEqual(first.Custom, map[string]any{"a": 1, "b": 1}) {
		t.Fatalf("the merge must not change an option set: %v", first.Custom)
	}

	worker := NewWorker("", WorkerOptions{})

	worker.On("x", func(context.Context, *Job) error { return nil }, first, second)

	var topics []string

	for _, subscription := range worker.subscriptions {
		topics = append(topics, subscription.Topic)

		if subscription.Retry == nil || *subscription.Retry != 0 {
			t.Fatalf("the subscription must have the later retry: %+v", subscription)
		}

		want := Subscription{Topic: subscription.Topic, Queue: "high", Event: "x", Retry: subscription.Retry, ReserveFor: 120}

		if subscription != want {
			t.Fatalf("unexpected subscription: %+v", subscription)
		}
	}

	if !slices.Equal(topics, []string{"t1", "t2", "t3"}) {
		t.Fatalf("topics must add up in order, with no duplicates: %v", topics)
	}
}

func TestI03SecondDeclarationForAnEventIsAnError(t *testing.T) {
	worker := NewWorker("", WorkerOptions{})
	handler := func(context.Context, *Job) error { return nil }

	worker.On("email.send", handler)
	worker.On("email.store", handler)

	defer func() {
		message, _ := recover().(string)

		if !strings.Contains(message, `"email.send"`) {
			t.Fatalf("a second On for an event must panic with the event: %q", message)
		}
	}()

	worker.On("email.send", handler, Options{Queue: "other"})
}

func TestI04DeclarationWithTopicsSubscribesAndRunsCopies(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})
	editTopic := "chat.edit"
	copies := make(chan *Job, 2)

	options := Options{
		Queue:      "audit",
		Retry:      Int(2),
		ReserveFor: 90 * time.Second,
		Topics:     []string{chatTopic, editTopic},
	}

	worker.On("chat.audit", func(_ context.Context, job *Job) error {
		if event, _ := job.Custom["event"].(string); strings.HasPrefix(event, "msg-") {
			copies <- job
		}

		return nil
	}, options)

	runWorker(t, worker)

	for _, topic := range options.Topics {
		waitFor(t, func() bool {
			published, err := client.Publish(context.Background(), topic, nil, PublishOptions{})

			return err == nil && published.Jobs == 1
		})
	}

	for i, topic := range options.Topics {
		publishOptions := PublishOptions{ID: fmt.Sprintf("msg-%d", i)}

		if _, err := client.Publish(context.Background(), topic, []any{"hi"}, publishOptions); err != nil {
			t.Fatal(err)
		}
	}

	seen := map[string]bool{}

	for range options.Topics {
		var job *Job

		select {
		case job = <-copies:
		case <-time.After(5 * time.Second):
			t.Fatalf("the worker ran only the copies of %v", seen)
		}

		if job.Type != "chat.audit" || job.Queue != "audit" || job.Retry == nil || *job.Retry != 2 || job.ReserveFor != 90 {
			t.Fatalf("the copy must have the options of the declaration: %+v", job)
		}

		seen[job.Custom["topic"].(string)] = true
	}

	if !seen[chatTopic] || !seen[editTopic] {
		t.Fatalf("the worker must run one copy of each topic: %v", seen)
	}
}
