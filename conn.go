package anway

import (
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const protocolVersion = 2

type hello struct {
	Version  int      `json:"v"`
	PwdHash  string   `json:"pwdhash,omitempty"`
	Hostname string   `json:"hostname,omitempty"`
	WID      string   `json:"wid,omitempty"`
	PID      int      `json:"pid,omitempty"`
	Labels   []string `json:"labels,omitempty"`

	Concurrency    int        `json:"concurrency,omitempty"`
	Tag            string     `json:"tag,omitempty"`
	Queues         []string   `json:"queues,omitempty"`
	Autoscale      *Autoscale `json:"autoscale,omitempty"`
	MaxConcurrency int        `json:"max_concurrency,omitempty"`
}

type conn struct {
	raw    net.Conn
	reader *bufio.Reader
}

func dial(ctx context.Context, target endpoint, options ClientOptions) (*conn, error) {
	dialer := &net.Dialer{Timeout: options.Timeout}

	raw, err := dialer.DialContext(ctx, "tcp", target.address)
	if err != nil {
		return nil, err
	}

	if target.tls {
		config := options.TLS
		if config == nil {
			config = &tls.Config{MinVersion: tls.VersionTLS12}
		}

		config = config.Clone()

		if config.ServerName == "" {
			config.ServerName, _, _ = net.SplitHostPort(target.address)
		}

		raw = tls.Client(raw, config)
	}

	current := &conn{raw: raw, reader: bufio.NewReader(raw)}

	raw.SetDeadline(time.Now().Add(options.Timeout))

	if err := current.handshake(target, options); err != nil {
		raw.Close()

		return nil, err
	}

	raw.SetDeadline(time.Time{})

	return current, nil
}

func (c *conn) handshake(target endpoint, options ClientOptions) error {
	greeting, err := c.read()
	if err != nil {
		return err
	}

	var hi struct {
		Version    int    `json:"v"`
		Salt       string `json:"s"`
		Iterations int    `json:"i"`
	}

	if err := json.Unmarshal([]byte(strings.TrimPrefix(greeting, "HI ")), &hi); err != nil {
		return fmt.Errorf("invalid greeting: %w", err)
	}

	if hi.Version != protocolVersion {
		return fmt.Errorf("unsupported protocol version %d", hi.Version)
	}

	request := hello{Version: protocolVersion}

	if hi.Salt != "" {
		request.PwdHash = passwordHash(target.password, hi.Salt, hi.Iterations)
	}

	if options.WID != "" {
		request.WID = options.WID
		request.Hostname, _ = os.Hostname()
		request.PID = os.Getpid()
		request.Labels = options.Labels
	}

	if worker := options.identity; worker != nil {
		if worker.Hostname != "" {
			request.Hostname = worker.Hostname
		}

		request.Concurrency = worker.Concurrency
		request.Tag = worker.Tag
		request.Queues = worker.Queues
		request.Autoscale = worker.Autoscale
		request.MaxConcurrency = worker.MaxConcurrency
	}

	_, err = c.command("HELLO", request)

	return err
}

func passwordHash(password, salt string, iterations int) string {
	digest := sha256.Sum256([]byte(password + salt))

	for i := 1; i < iterations; i++ {
		digest = sha256.Sum256(digest[:])
	}

	return hex.EncodeToString(digest[:])
}

// command sends a verb with a string or JSON argument, and returns the reply.
func (c *conn) command(verb string, argument any) (string, error) {
	line := verb

	switch value := argument.(type) {
	case nil:
	case string:
		line += " " + value
	default:
		encoded, err := json.Marshal(value)
		if err != nil {
			return "", err
		}

		line += " " + string(encoded)
	}

	if _, err := io.WriteString(c.raw, line+"\r\n"); err != nil {
		return "", &transportError{err}
	}

	return c.read()
}

func (c *conn) read() (string, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", &transportError{err}
	}

	line = strings.TrimRight(line, "\r\n")

	if line == "" {
		return "", &transportError{errors.New("empty reply")}
	}

	body := line[1:]

	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return "", parseError(body)
	case '$':
		return c.bulk(body)
	default:
		return "", &transportError{fmt.Errorf("unexpected reply: %q", line)}
	}
}

func (c *conn) bulk(size string) (string, error) {
	length, err := strconv.Atoi(size)
	if err != nil {
		return "", &transportError{err}
	}

	if length < 0 {
		return "", nil
	}

	body := make([]byte, length+2)

	if _, err := io.ReadFull(c.reader, body); err != nil {
		return "", &transportError{err}
	}

	return string(body[:length]), nil
}

func (c *conn) close() {
	c.raw.Close()
}
