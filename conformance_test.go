package anway

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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

	client, err := Open(serverURL, Options{WID: fmt.Sprintf("test-%d", time.Now().UnixNano())})
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { client.Close() })

	return client
}

func newJob(t *testing.T, jobType string, args ...any) *Job {
	t.Helper()

	job, err := NewJob(jobType, args...)
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

	options := Options{TLS: &tls.Config{RootCAs: roots, ServerName: "example.com"}}

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

	if err := client.Push(ctx, newJob(t, "email", "a@b.com", 42)); err != nil {
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
	pushed := newJob(t, "x")

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

	if err := client.Push(ctx, newJob(t, "x", 1).SetCustom("unique_for", 60)); err != nil {
		t.Fatal(err)
	}

	err := client.Push(ctx, newJob(t, "x", 1).SetCustom("unique_for", 60))
	if !errors.Is(err, ErrNotUnique) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestC08Scheduled(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x").RunAt(time.Now().Add(time.Hour))); err != nil {
		t.Fatal(err)
	}

	if job, err := client.Fetch(ctx, "default"); job != nil || err != nil {
		t.Fatalf("unexpected result: %+v %v", job, err)
	}
}

func TestC09PauseResume(t *testing.T) {
	client := open(t, startServer(t).url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x")); err != nil {
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
		pushed := newJob(t, kind)

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

	if err := client.Mutate(ctx, "requeue", "retries", Filter{JobType: "a"}); err != nil {
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

	bid, err := client.BatchNew(ctx, Batch{Success: newJob(t, "on-success"), Complete: newJob(t, "on-complete")})
	if err != nil {
		t.Fatal(err)
	}

	if err := client.Push(ctx, newJob(t, "member").SetCustom("bid", bid)); err != nil {
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
	pushed := newJob(t, "x").SetCustom("track", 1)

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

	if err := client.Push(ctx, newJob(t, "x")); err != nil {
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
			if err := client.Push(ctx, newJob(t, "x")); err != nil {
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

	if err := client.Push(context.Background(), newJob(t, "x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestC15Reconnect(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	ctx := context.Background()

	if err := client.Push(ctx, newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	server.stop()
	server.start(t)

	var err error

	for range 3 {
		if err = client.Push(ctx, newJob(t, "x")); err == nil {
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

	worker.Register("x", func(context.Context, *Job) error { return nil })
	runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x")); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return processed(t, client) == 1 })
}

func TestW02W03W04FailureKinds(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	worker := NewWorker(server.url(), WorkerOptions{Concurrency: 1})

	worker.Register("error", func(context.Context, *Job) error { return errors.New("failed") })
	worker.Register("panic", func(context.Context, *Job) error { panic("boom") })
	worker.Register("ok", func(context.Context, *Job) error { return nil })
	runWorker(t, worker)

	for _, kind := range []string{"error", "panic", "unknown", "ok"} {
		if err := client.Push(context.Background(), newJob(t, kind)); err != nil {
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

	worker.Register("x", func(ctx context.Context, _ *Job) error {
		result <- ctx.Value(contextKey("b")).(string)

		return nil
	})

	runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "x")); err != nil {
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

	worker.Register("slow", func(ctx context.Context, _ *Job) error {
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

	worker.Register("slow", func(context.Context, *Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		finished.Store(true)

		return nil
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow")); err != nil {
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
// The first FETCH gives out job, when it is set. FAIL reports are recorded.
type scriptedServer struct {
	job       string
	lastQuiet int64

	beats        atomic.Int64
	quietFetches atomic.Int64
	jobSent      atomic.Bool
	terminate    atomic.Bool

	mu     sync.Mutex
	failed []failReport
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
		case "BEAT":
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

	worker.Register("slow", func(ctx context.Context, _ *Job) error {
		close(started)
		<-ctx.Done()

		return ctx.Err()
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "slow")); err != nil {
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

	worker.Register("stuck", func(context.Context, *Job) error {
		defer close(returned)

		close(started)
		<-release

		return nil
	})

	cancel, done := runWorker(t, worker)

	if err := client.Push(context.Background(), newJob(t, "stuck")); err != nil {
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

const chatTopic = "chat.message"

func subscribe(t *testing.T, client *Client, queue, jobType string) {
	t.Helper()

	subscription := Subscription{Topic: chatTopic, Queue: queue, JobType: jobType}

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

	for queue, jobType := range subscribers {
		subscribe(t, client, queue, jobType)
	}

	options := PublishOptions{ID: "msg-42", Custom: map[string]any{"tenant": "acme"}}
	published := publish(t, client, options, map[string]any{"room": "general", "body": "hi"})

	if published != (Published{Event: "msg-42", Jobs: 3}) {
		t.Fatalf("unexpected result: %+v", published)
	}

	for queue, jobType := range subscribers {
		job, err := client.Fetch(ctx, queue)
		if err != nil {
			t.Fatal(err)
		}

		if job == nil || job.Type != jobType || job.Queue != queue {
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

	worker.Subscribe(Subscription{Topic: chatTopic, Queue: "realtime", JobType: "chat.broadcast"}, log.handler("msg-"))
	runWorker(t, worker)

	waitSubscribed(t, client, 1)
	publish(t, client, PublishOptions{ID: "msg-1"}, "hi")

	waitFor(t, func() bool { return log.snapshot()["msg-1"] == 1 })
}

func TestT06TwoWorkersRunOneCopyOfEachEvent(t *testing.T) {
	server := startServer(t)
	client := open(t, server.url())
	logs := map[string]*eventLog{}

	for queue, jobType := range map[string]string{"cassandra": "chat.store", "push": "chat.notify"} {
		worker := NewWorker(server.url(), WorkerOptions{Concurrency: 2})
		logs[queue] = &eventLog{counts: map[string]int{}}

		worker.Subscribe(Subscription{Topic: chatTopic, Queue: queue, JobType: jobType}, logs[queue].handler("msg-"))
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
