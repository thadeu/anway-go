package anway

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Failure holds the data of the last failed run.
type Failure struct {
	RetryCount int      `json:"retry_count"`
	FailedAt   string   `json:"failed_at"`
	NextAt     string   `json:"next_at,omitempty"`
	Message    string   `json:"message,omitempty"`
	ErrType    string   `json:"errtype,omitempty"`
	Backtrace  []string `json:"backtrace,omitempty"`
}

// Job is one unit of work.
type Job struct {
	JID        string          `json:"jid"`
	Type       string          `json:"jobtype"`
	Args       json.RawMessage `json:"args"`
	Queue      string          `json:"queue,omitempty"`
	ReserveFor int             `json:"reserve_for,omitempty"`
	At         string          `json:"at,omitempty"`
	Retry      *int            `json:"retry,omitempty"`
	Backtrace  int             `json:"backtrace,omitempty"`
	CreatedAt  string          `json:"created_at,omitempty"`
	EnqueuedAt string          `json:"enqueued_at,omitempty"`
	Failure    *Failure        `json:"failure,omitempty"`
	Custom     map[string]any  `json:"custom,omitempty"`
}

// NewJob creates a job for the default queue with a generated id.
func NewJob(jobType string, args ...any) (*Job, error) {
	if args == nil {
		args = []any{}
	}

	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("encode args: %w", err)
	}

	jid, err := randomID()
	if err != nil {
		return nil, err
	}

	return &Job{JID: jid, Type: jobType, Args: encoded}, nil
}

// Bind decodes the args in order into the given pointers.
func (j *Job) Bind(targets ...any) error {
	var values []json.RawMessage

	if err := json.Unmarshal(j.Args, &values); err != nil {
		return err
	}

	if len(targets) > len(values) {
		return fmt.Errorf("job has %d args, want %d", len(values), len(targets))
	}

	for i, target := range targets {
		if err := json.Unmarshal(values[i], target); err != nil {
			return fmt.Errorf("arg %d: %w", i, err)
		}
	}

	return nil
}

// RunAt schedules the job.
func (j *Job) RunAt(at time.Time) *Job {
	j.At = at.UTC().Format(time.RFC3339Nano)

	return j
}

// SetCustom adds one custom field.
func (j *Job) SetCustom(key string, value any) *Job {
	if j.Custom == nil {
		j.Custom = map[string]any{}
	}

	j.Custom[key] = value

	return j
}

func randomID() (string, error) {
	raw := make([]byte, 12)

	if _, err := rand.Read(raw); err != nil {
		return "", err
	}

	return hex.EncodeToString(raw), nil
}
