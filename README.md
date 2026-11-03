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

## Topics

A topic sends one event to many queues. `Publish` makes one job for each subscription of the topic, in one transaction. Each copy is a normal job in the queue of its subscriber, with its own retries.

Publish one `chat.message` event:

```go
published, err := client.Publish(ctx, "chat.message", []any{map[string]any{"room": "general", "body": "hi"}}, anway.PublishOptions{ID: "msg-42"})
if err != nil {
	return err
}

// published.Jobs is 3: one job in each of the realtime, cassandra, and push queues.
```

Each app declares its subscription in its worker, with its own queue and job type. In this short example, one worker has all three:

```go
worker := anway.NewWorker("", anway.WorkerOptions{})
retries := 5

worker.Subscribe(anway.Subscription{Topic: "chat.message", Queue: "realtime", JobType: "chat.broadcast"}, broadcast)
worker.Subscribe(anway.Subscription{Topic: "chat.message", Queue: "cassandra", JobType: "chat.store"}, store)
worker.Subscribe(anway.Subscription{Topic: "chat.message", Queue: "push", JobType: "chat.notify", Retry: &retries}, notify)

return worker.Run(ctx)
```

`Run` sends `SUBSCRIBE` for each subscription before its first fetch. The worker fetches from the queue of each subscription. Without `Queues`, it fetches only from these queues.

- With an `ID`, a second event with the same topic and id within `UniqueFor` (one day) does nothing, and `Duplicate` is true. A zero `UniqueFor` turns this check off.
- `Custom` goes to each copy. The server adds `topic` and `event` to it. Use `job.Custom["event"]` to make a handler idempotent.
- An event with no subscription goes to the Dead set, and `Jobs` is 0.
- `Retry` and `ReserveFor` in a `Subscription` set the options of each copy. `Worker.Subscribe` sends them at each start, so the worker code sets them. `client.Subscribe` and `client.Unsubscribe` change subscriptions from any app.

## Contract

| Group | Operations |
|---|---|
| Jobs | `Push`, `Fetch`, `Ack`, `Fail`, `Beat` |
| Queues | `PauseQueues`, `ResumeQueues`, `RemoveQueues`, `QueueSizes`, `Info` |
| Admin | `Mutate` |
| Batches | `BatchNew`, `BatchOpen`, `BatchCommit`, `BatchStatus` |
| Tracking | `TrackGet`, `TrackSet` |
| Topics | `Publish`, `Subscribe`, `Unsubscribe` |
| Worker | `Register`, `Subscribe`, `Use`, `Run` |

The error kinds are `ErrNotUnique`, `ErrClosed`, `ServerError`, and connection errors. A failed call does not resend, so the app decides whether to repeat it.

## Tests

```bash
SERVER_BIN=/path/to/anway go test -race ./...
```

The tests map to the [conformance scenarios](https://github.com/thadeu/anway/blob/main/conformance/scenarios.md).

## License

MIT
