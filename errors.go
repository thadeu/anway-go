package anway

import (
	"errors"
	"strings"
)

// ErrNotUnique reports that a unique lock blocked a push.
var ErrNotUnique = errors.New("anway: job not unique")

// ErrClosed reports a call on a closed client.
var ErrClosed = errors.New("anway: client is closed")

// ServerError is an error reply from the server.
type ServerError struct {
	Code    string
	Message string
}

func (e *ServerError) Error() string {
	return "anway: " + e.Code + " " + e.Message
}

// transportError marks a connection that must not be reused.
type transportError struct {
	err error
}

func (e *transportError) Error() string {
	return "anway: connection failed: " + e.err.Error()
}

func (e *transportError) Unwrap() error {
	return e.err
}

func parseError(body string) error {
	code, message, _ := strings.Cut(body, " ")

	if code == "NOTUNIQUE" {
		return ErrNotUnique
	}

	return &ServerError{Code: code, Message: message}
}
