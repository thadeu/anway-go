<p>
  <a href="https://github.com/thadeu/anway">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/thadeu/anway/main/brand/png/lockup-on-dark.png">
      <img src="https://raw.githubusercontent.com/thadeu/anway/main/brand/png/lockup-on-light.png" alt="Anway" width="320">
    </picture>
  </a>
</p>

# anway-go

Go client and worker for [Anway](https://github.com/thadeu/anway). This client sets the contract for all Anway clients.

```bash
go get github.com/thadeu/anway-go
```

## Push

```go
client, err := anway.Open("tcp://:password@localhost:7419", anway.Options{})
if err != nil {
	return err
}
defer client.Close()

job, err := anway.NewJob("email.send", "a@b.com", "Welcome")
if err != nil {
	return err
}

return client.Push(ctx, job)
```

A blank URL uses `ANWAY_URL`. Use `tcp+tls://` for TLS. A client is safe for concurrent use.

## Worker

```go
worker := anway.NewWorker("", anway.WorkerOptions{Queues: []string{"critical", "default"}})

worker.Register("email.send", func(ctx context.Context, job *anway.Job) error {
	var address, subject string

	if err := job.Bind(&address, &subject); err != nil {
		return err
	}

	return send(ctx, address, subject)
})

return worker.Run(ctx)
```

A handler error or panic fails the job, and the server retries it. Delivery is at least once, so handlers must be idempotent.

### Shutdown

The worker stops at the server `terminate` command, or when its context ends. Server `quiet` stops fetching, and the worker stays alive.

1. Running jobs get `ShutdownTimeout` (25 seconds) to finish.
2. The worker cancels their contexts, and they get `CancelGrace` (5 seconds) to return.
3. The worker fails jobs that still run, and `Run` returns. A late result from these handlers is ignored.

Handlers must respect `ctx`. A goroutine that ignores it continues to run after `Run` returns.

## Contract

| Group | Operations |
|---|---|
| Jobs | `Push`, `Fetch`, `Ack`, `Fail`, `Beat` |
| Queues | `PauseQueues`, `ResumeQueues`, `RemoveQueues`, `QueueSizes`, `Info` |
| Admin | `Mutate` |
| Batches | `BatchNew`, `BatchOpen`, `BatchCommit`, `BatchStatus` |
| Tracking | `TrackGet`, `TrackSet` |
| Worker | `Register`, `Use`, `Run` |

The error kinds are `ErrNotUnique`, `ErrClosed`, `ServerError`, and connection errors. A failed call does not resend, so the app decides whether to repeat it.

## Tests

```bash
SERVER_BIN=/path/to/anway go test -race ./...
```

The tests map to the [conformance scenarios](https://github.com/thadeu/anway/blob/main/conformance/scenarios.md).

## License

MIT
