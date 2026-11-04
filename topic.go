package anway

import (
	"context"
	"fmt"
	"time"
)

// A topic sends one event to many queues. PUBLISH makes one job for each
// subscription of the topic. Each copy is a normal job in the queue of its
// subscriber.

// Published is the result of Publish: the event id, the number of jobs that
// the event made, and true when the event was a duplicate.
type Published struct {
	Event     string `json:"event"`
	Jobs      int    `json:"jobs"`
	Duplicate bool   `json:"duplicate"`
}

// PublishOptions changes an event. All fields are optional.
//
// ID is the event id. Without it, the server makes one. With it, a second
// event with the same topic and id within UniqueFor does nothing. UniqueFor
// has a server default of one day, and zero turns the check off. Custom goes
// to each copy, and the server adds "topic" and "event" to it.
type PublishOptions struct {
	ID        string
	UniqueFor *time.Duration
	Custom    map[string]any
}

// Subscription sends the events of a topic to a queue as jobs of an event.
// The event goes out as the job type. Retry and ReserveFor (in seconds) are
// the options of each copy, as in a job.
type Subscription struct {
	Topic      string `json:"topic"`
	Queue      string `json:"queue"`
	Event      string `json:"jobtype"`
	Retry      *int   `json:"retry,omitempty"`
	ReserveFor int    `json:"reserve_for,omitempty"`
}

type publishRequest struct {
	Topic     string         `json:"topic"`
	Args      []any          `json:"args"`
	ID        string         `json:"id,omitempty"`
	UniqueFor *float64       `json:"unique_for,omitempty"`
	Custom    map[string]any `json:"custom,omitempty"`
}

// Event is one event of PublishBulk: a topic, its args, and its options.
type Event struct {
	Topic   string
	Args    []any
	Options PublishOptions
}

// PublishResult is the result of one event of PublishBulk. Error is the
// reason when the server refused the event, and is empty in other cases.
type PublishResult struct {
	Published
	Error string `json:"error,omitempty"`
}

func newPublishRequest(topic string, args []any, options PublishOptions) publishRequest {
	if args == nil {
		args = []any{}
	}

	request := publishRequest{Topic: topic, Args: args, ID: options.ID, Custom: options.Custom}

	if options.UniqueFor != nil {
		seconds := options.UniqueFor.Seconds()
		request.UniqueFor = &seconds
	}

	return request
}

// Publish sends an event to every subscription of the topic, in one
// transaction. An event with no subscription goes to the dead set, and the
// result has 0 jobs.
func (c *Client) Publish(ctx context.Context, topic string, args []any, options PublishOptions) (Published, error) {
	var published Published

	return published, c.callJSON(ctx, "PUBLISH", newPublishRequest(topic, args, options), &published)
}

// PublishBulk sends up to MaxBulk events with one PUBLISHB, and the server
// saves them in one write. Each event works as one Publish. It returns one
// result for each event, in the same order. A refused event, such as a bad
// topic, has Error, and does not stop the others. More than MaxBulk events
// is an error, and nothing goes to the server.
func (c *Client) PublishBulk(ctx context.Context, events []Event) ([]PublishResult, error) {
	if len(events) > MaxBulk {
		return nil, fmt.Errorf("anway: a bulk publish has at most %d events, not %d", MaxBulk, len(events))
	}

	if len(events) == 0 {
		return []PublishResult{}, nil
	}

	requests := make([]publishRequest, len(events))

	for index, event := range events {
		requests[index] = newPublishRequest(event.Topic, event.Args, event.Options)
	}

	var results []PublishResult

	return results, c.callJSON(ctx, "PUBLISHB", requests, &results)
}

// Subscribe saves a subscription on the server. It is idempotent: a second
// call with the same topic, queue, and event sets the new options.
func (c *Client) Subscribe(ctx context.Context, subscription Subscription) error {
	_, err := c.call(ctx, "SUBSCRIBE", subscription, 0)

	return err
}

// Unsubscribe removes a subscription. A missing subscription is not an error.
func (c *Client) Unsubscribe(ctx context.Context, topic, queue, event string) error {
	subscription := Subscription{Topic: topic, Queue: queue, Event: event}

	_, err := c.call(ctx, "UNSUBSCRIBE", subscription, 0)

	return err
}
