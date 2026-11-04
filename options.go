package anway

import (
	"math"
	"slices"
	"time"
)

const defaultQueue = "default"

// Options holds the keys of the input contract, which are the same in all
// Anway clients. NewJob uses them for a push, and Worker.On uses them for a
// declaration. All fields are optional.
//
// Several sets merge in order. For a single value, the last set that sets it
// wins: Retry when it is not nil, Queue and UniqueUntil when they are not
// blank, OnDead when it is not nil, Track when it is true, and the other
// fields when they are not zero. Topics add up in order, with no duplicates.
// Custom maps merge, and a later key wins.
//
// Durations go out in seconds. UniqueFor, UniqueUntil, ExpiresIn, ExpiresAt,
// Track, and OnDead go to the custom hash of the job, and they win over a
// Custom key with the same name.
type Options struct {
	Queue       string
	Retry       *int
	ReserveFor  time.Duration
	Backtrace   int
	At          time.Time
	UniqueFor   time.Duration
	UniqueUntil string
	ExpiresIn   time.Duration
	ExpiresAt   time.Time
	Track       bool
	OnDead      *DeadHook
	Custom      map[string]any
	Topics      []string
}

// DeadHook names the job that the server pushes when a job goes to the Dead
// set. Event is its job type. A blank Queue uses the server default.
type DeadHook struct {
	Event string
	Queue string
}

// Int returns a pointer to n, for Options.Retry and Subscription.Retry.
func Int(n int) *int {
	return &n
}

// translate merges the option sets in order and turns the contract keys into
// the wire fields of a job for the event. It also returns the merged topics,
// which only a declaration uses.
func translate(event string, sets []Options) (Job, []string) {
	merged := mergeOptions(sets)

	job := Job{
		Type:       event,
		Queue:      merged.Queue,
		Retry:      merged.Retry,
		ReserveFor: seconds(merged.ReserveFor),
		Backtrace:  merged.Backtrace,
	}

	if job.Queue == "" {
		job.Queue = defaultQueue
	}

	if !merged.At.IsZero() {
		job.At = formatTime(merged.At)
	}

	custom := merged.Custom

	put := func(key string, value any) {
		if custom == nil {
			custom = map[string]any{}
		}

		custom[key] = value
	}

	if merged.UniqueFor != 0 {
		put("unique_for", merged.UniqueFor.Seconds())
	}

	if merged.UniqueUntil != "" {
		put("unique_until", merged.UniqueUntil)
	}

	if merged.ExpiresIn != 0 {
		put("expires_in", merged.ExpiresIn.Seconds())
	}

	if !merged.ExpiresAt.IsZero() {
		put("expires_at", formatTime(merged.ExpiresAt))
	}

	if merged.Track {
		put("track", 1)
	}

	if merged.OnDead != nil {
		hook := map[string]any{"jobtype": merged.OnDead.Event}

		if merged.OnDead.Queue != "" {
			hook["queue"] = merged.OnDead.Queue
		}

		put("on_dead", hook)
	}

	job.Custom = custom

	return job, merged.Topics
}

// mergeOptions merges the option sets in order. The result does not share
// a map, a slice, or a pointer with the sets.
func mergeOptions(sets []Options) Options {
	var merged Options

	for _, set := range sets {
		if set.Queue != "" {
			merged.Queue = set.Queue
		}

		if set.Retry != nil {
			merged.Retry = Int(*set.Retry)
		}

		if set.ReserveFor != 0 {
			merged.ReserveFor = set.ReserveFor
		}

		if set.Backtrace != 0 {
			merged.Backtrace = set.Backtrace
		}

		if !set.At.IsZero() {
			merged.At = set.At
		}

		if set.UniqueFor != 0 {
			merged.UniqueFor = set.UniqueFor
		}

		if set.UniqueUntil != "" {
			merged.UniqueUntil = set.UniqueUntil
		}

		if set.ExpiresIn != 0 {
			merged.ExpiresIn = set.ExpiresIn
		}

		if !set.ExpiresAt.IsZero() {
			merged.ExpiresAt = set.ExpiresAt
		}

		if set.Track {
			merged.Track = true
		}

		if set.OnDead != nil {
			hook := *set.OnDead
			merged.OnDead = &hook
		}

		for key, value := range set.Custom {
			if merged.Custom == nil {
				merged.Custom = map[string]any{}
			}

			merged.Custom[key] = value
		}

		for _, topic := range set.Topics {
			if !slices.Contains(merged.Topics, topic) {
				merged.Topics = append(merged.Topics, topic)
			}
		}
	}

	return merged
}

// seconds rounds a duration up to whole seconds, because the reserve_for
// field of the protocol is an integer.
func seconds(duration time.Duration) int {
	return int(math.Ceil(duration.Seconds()))
}

func formatTime(at time.Time) string {
	return at.UTC().Format(time.RFC3339Nano)
}
