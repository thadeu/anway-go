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
client, err := anway.Open("tcp://:password@localhost:7419", anway.ClientOptions{})
if err != nil {
	return err
}
defer client.Close()

job, err := anway.NewJob("email.send", []any{"a@b.com", "Welcome"}, anway.Options{Queue: "critical", Retry: anway.Int(5)})
if err != nil {
	return err
}

return client.Push(ctx, job)
```

A blank URL uses `ANWAY_URL`. Use `tcp+tls://` for TLS. A client is safe for concurrent use.

### Bulk push

`PushBulk` sends up to 1,000 jobs (`anway.MaxBulk`) with one `PUSHB`. The server saves them in one write.

```go
failures, err := client.PushBulk(ctx, jobs)
if err != nil {
	return err
}

for jid, reason := range failures {
	log.Printf("job %s is not in: %s", jid, reason)
}
```

- `failures` holds the reason for each job that is not in, by jid, such as a duplicate jid or a unique lock. It is empty when all jobs are in.
- A refused job does not stop the others.
- More than 1,000 jobs is an error, and no job goes to the server. Send more jobs in parts of 1,000 or less.

## Worker

```go
worker := anway.NewWorker("", anway.WorkerOptions{Queues: []string{"critical", "default"}})

worker.On("email.send", func(ctx context.Context, job *anway.Job) error {
	var address, subject string

	if err := job.Bind(&address, &subject); err != nil {
		return err
	}

	return send(ctx, address, subject)
})

return worker.Run(ctx)
```

A handler error or panic fails the job, and the server retries it. Delivery is at least once, so handlers must be idempotent.

A worker has one handler for each event. A second `On` for the same event panics, as a duplicate pattern does in `net/http`.

### Shutdown

The worker stops at the server `terminate` command, or when its context ends. Server `quiet` stops fetching, and the worker stays alive.

1. Running jobs get `ShutdownTimeout` (25 seconds) to finish.
2. The worker cancels their contexts, and they get `CancelGrace` (5 seconds) to return.
3. The worker fails jobs that still run, and `Run` returns. A late result from these handlers is ignored.

Handlers must respect `ctx`. A goroutine that ignores it continues to run after `Run` returns.

### Threads and autoscale

A worker runs `Concurrency` jobs at once (default 10), each in its own goroutine. The web UI calls them threads. The server can change this count while the worker runs: from the web UI, from the HTTP API, or with autoscale. See [worker threads](https://github.com/thadeu/anway/blob/main/docs/dev/worker-threads.md).

```go
worker := anway.NewWorker("", anway.WorkerOptions{
	Queues:         []string{"critical", "default"},
	Concurrency:    10,
	MaxConcurrency: 25,
	Autoscale:      &anway.Autoscale{Max: 40},
	Hostname:       "worker.1",
	Tag:            "shop",
})
```

| Field | Default | Note |
|---|---|---|
| `Concurrency` | 10 | The jobs that the worker runs at once when it starts. |
| `MaxConcurrency` | 0 (no limit) | The most jobs that the worker can run at once, such as the size of its database pool. The worker never runs more, and the server never asks for more. |
| `Autoscale` | nil (off) | `&anway.Autoscale{Min, Max}` starts the worker with autoscale on. A zero `Min` is `Concurrency`. The server lowers a `Max` above `MaxConcurrency` to it. |
| `Hostname` | The machine name | The web UI shows the worker by hostname and tag. |
| `Tag` | None | A short name for the app or the role of the process, such as `shop`. |

How the worker obeys the server:

- Each `BEAT` sends the count that runs now, also the first `BEAT` at start. `HELLO` sends the hostname, pid, `Concurrency`, queues, tag, autoscale, and `MaxConcurrency`. A field that is not set is not sent.
- When the server asks for more threads, the worker starts goroutines at once. When it asks for fewer, goroutines stop after their job. The worker never stops a running job to scale down.
- A stop that is still pending is canceled first when the server asks for more threads again.
- The connection pool grows with the count. It does not shrink.
- After a change, the worker sends a `BEAT` with the new count at once, not at the next interval.
- After `quiet`, the worker does not change its threads.

The server saves a change for the worker name: hostname and tag. A new process with that name starts with the saved setting, not with its config. Set a stable `Hostname` in containers, so the setting stays across deploys.

Each goroutine can hold a database connection. Set `MaxConcurrency` to the pool size, or set `Autoscale.Max` at or below it. Otherwise jobs can fail with a pool timeout.

## Options

Since v0.4.0, `NewJob` and `Worker.On` take `anway.Options`, with the keys of the [input contract](https://github.com/thadeu/anway/blob/main/docs/dev/clients.md#input-contract). All Anway clients use the same keys. The client translates them to the fields of the protocol.

```go
job, err := anway.NewJob("report.build", []any{42}, anway.Options{
	Queue:     "reports",
	At:        time.Now().Add(time.Hour),
	UniqueFor: 10 * time.Minute,
	Track:     true,
	OnDead:    &anway.DeadHook{Event: "report.failed"},
	Custom:    map[string]any{"tenant": "acme"},
})
```

| Field | Wire field | Note |
|---|---|---|
| `Queue` | `queue` | Default `default`. |
| `Retry` | `retry` | Use `anway.Int(n)`. Default 25 (server). `0` discards at the first failure. `-1` goes to Dead at once. |
| `ReserveFor` | `reserve_for` | Sent in whole seconds, rounded up. |
| `Backtrace` | `backtrace` | Lines. |
| `At` | `at` | For a push only. |
| `UniqueFor` | `custom.unique_for` | Sent in seconds. |
| `UniqueUntil` | `custom.unique_until` | `success` or `start`. |
| `ExpiresIn` | `custom.expires_in` | Sent in seconds. |
| `ExpiresAt` | `custom.expires_at` | RFC 3339. |
| `Track` | `custom.track` | `true` sends `1`. |
| `OnDead` | `custom.on_dead` | `DeadHook{Event, Queue}`. `Event` goes out as `jobtype`. |
| `Custom` | `custom` | The keys of the app. The keys above win over a key with the same name. |
| `Topics` | One `SUBSCRIBE` for each topic | For `On` only. |

The event, which is the first argument of `NewJob` and `On`, goes out as `jobtype`.

You can give more than one `Options` value. They merge in order:

- A single value, such as `Queue` or `Retry`: the last value that is set wins.
- `Topics` add up, in order, with no duplicates.
- `Custom` maps merge, and a later key wins.

```go
defaults := anway.Options{Queue: "mail", Retry: anway.Int(3)}

job, err := anway.NewJob("email.send", []any{"a@b.com"}, defaults, anway.Options{Retry: anway.Int(10)})
```

`On` uses only `Queue`, `Retry`, `ReserveFor`, and `Topics`. The other fields are for a push, and have no effect in `On`.

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

### Bulk publish

`PublishBulk` sends up to 1,000 events (`anway.MaxBulk`) with one `PUBLISHB`. The server saves all copies in one write, so an import or an outbox relay is much faster than one `Publish` for each event. Use `Publish` for one event, such as in a request.

```go
results, err := client.PublishBulk(ctx, []anway.Event{
	{Topic: "order.paid", Args: []any{order1}, Options: anway.PublishOptions{ID: "order-1"}},
	{Topic: "order.paid", Args: []any{order2}, Options: anway.PublishOptions{ID: "order-2"}},
})
if err != nil {
	return err
}

for _, result := range results {
	if result.Error != "" {
		log.Printf("event %s: %s", result.Event, result.Error)
	}
}
```

The results keep the order of the events. Each event works as one `Publish`, with the same dedup by id. A refused event, such as a bad topic, has `Error` and does not stop the others. A bulk can make at most 10,000 jobs over all subscriptions.

Each app declares its subscription in its worker, with the `Topics` option, and its own event and queue. In this short example, one worker has all three:

```go
worker := anway.NewWorker("", anway.WorkerOptions{})
topics := []string{"chat.message"}

worker.On("chat.broadcast", broadcast, anway.Options{Queue: "realtime", Topics: topics})
worker.On("chat.store", store, anway.Options{Queue: "cassandra", Topics: topics})
worker.On("chat.notify", notify, anway.Options{Queue: "push", Retry: anway.Int(5), Topics: topics})

return worker.Run(ctx)
```

`Run` sends `SUBSCRIBE` for each topic of each declaration before its first fetch, with the event as the job type, and the `Queue`, `Retry`, and `ReserveFor` of the declaration. When a declaration has topics or sets a queue, the worker fetches from that queue. Without `Queues` and without such a declaration, the worker fetches from `default`.

- With an `ID`, a second event with the same topic and id within `UniqueFor` (one day) does nothing, and `Duplicate` is true. A zero `UniqueFor` turns this check off.
- `Custom` goes to each copy. The server adds `topic` and `event` to it. Use `job.Custom["event"]` to make a handler idempotent.
- An event with no subscription goes to the Dead set, and `Jobs` is 0.
- `Run` sends the subscriptions again at each start, so the worker code sets them. To send one topic to several flows, declare several events, each with the topic.
- `client.Subscribe(ctx, anway.Subscription{Topic, Queue, Event, Retry, ReserveFor})` and `client.Unsubscribe(ctx, topic, queue, event)` change subscriptions from any app. `ReserveFor` in a `Subscription` is in seconds.

## Contract

| Group | Operations |
|---|---|
| Jobs | `NewJob`, `Push`, `PushBulk`, `Fetch`, `Ack`, `Fail`, `Beat`, `Heartbeat` |
| Queues | `PauseQueues`, `ResumeQueues`, `RemoveQueues`, `QueueSizes`, `Info` |
| Admin | `Mutate` |
| Batches | `BatchNew`, `BatchOpen`, `BatchCommit`, `BatchStatus` |
| Tracking | `TrackGet`, `TrackSet` |
| Topics | `Publish`, `PublishBulk`, `Subscribe`, `Unsubscribe` |
| Worker | `On`, `Use`, `Run` |

The input keys of `NewJob` and `On` are in [Options](#options). The error kinds are `ErrNotUnique`, `ErrClosed`, `ServerError`, and connection errors. A failed call does not resend, so the app decides whether to repeat it.

## Tests

```bash
SERVER_BIN=/path/to/anway go test -race ./...
```

The tests map to the [conformance scenarios](https://github.com/thadeu/anway/blob/main/conformance/scenarios.md).

## License

MIT
