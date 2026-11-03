package anway

import (
	"context"
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

// Subscription sends the events of a topic to a queue as jobs of a job type.
// Retry and ReserveFor are the options of each copy, as in a job.
type Subscription struct {
	Topic      string `json:"topic"`
	Queue      string `json:"queue"`
	JobType    string `json:"jobtype"`
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

// Publish sends an event to every subscription of the topic, in one
// transaction. An event with no subscription goes to the dead set, and the
// result has 0 jobs.
func (c *Client) Publish(ctx context.Context, topic string, args []any, options PublishOptions) (Published, error) {
	if args == nil {
		args = []any{}
	}

	request := publishRequest{Topic: topic, Args: args, ID: options.ID, Custom: options.Custom}

	if options.UniqueFor != nil {
		seconds := options.UniqueFor.Seconds()
		request.UniqueFor = &seconds
	}

	var published Published

	return published, c.callJSON(ctx, "PUBLISH", request, &published)
}

// Subscribe saves a subscription on the server. It is idempotent: a second
// call with the same topic, queue, and job type sets the new options.
func (c *Client) Subscribe(ctx context.Context, subscription Subscription) error {
	_, err := c.call(ctx, "SUBSCRIBE", subscription, 0)

	return err
}

// Unsubscribe removes a subscription. A missing subscription is not an error.
func (c *Client) Unsubscribe(ctx context.Context, topic, queue, jobType string) error {
	subscription := Subscription{Topic: topic, Queue: queue, JobType: jobType}
	_, err := c.call(ctx, "UNSUBSCRIBE", subscription, 0)

	return err
}
