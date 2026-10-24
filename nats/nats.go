package nats

import (
	"errors"
	"fmt"
	"log"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

type Config struct {
	URL  string
	Name string
}

type Client struct {
	conn   *natsgo.Conn
	closed chan struct{}
}

func New(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		cfg.URL = natsgo.DefaultURL
	}

	if cfg.Name == "" {
		return nil, errors.New("nats: client name cannot be empty")
	}

	closed := make(chan struct{})

	conn, err := natsgo.Connect(
		cfg.URL,
		natsgo.Name(cfg.Name),
		natsgo.MaxReconnects(-1),
		natsgo.ReconnectWait(3*time.Second),
		natsgo.ReconnectJitter(
			500*time.Millisecond,
			2*time.Second,
		),
		natsgo.RetryOnFailedConnect(true),
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			log.Printf("nats: disconnected: %v", err)
		}),
		natsgo.ReconnectHandler(func(c *natsgo.Conn) {
			log.Printf("nats: reconnected to %s", c.ConnectedUrl())
		}),
		natsgo.ClosedHandler(func(c *natsgo.Conn) {
			log.Printf("nats: connection closed: %v", c.LastError())
			close(closed)
		}),
		natsgo.ConnectHandler(func(c *natsgo.Conn) {
			log.Printf("nats: connected to %s", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn:   conn,
		closed: closed,
	}, nil
}

// Close drains the connection: in-flight and pending messages are handled
// and buffered publishes are flushed before it closes. It blocks until the
// connection is closed, so do not call it from inside a message handler.
func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	if err := c.conn.Drain(); err != nil {
		if errors.Is(err, natsgo.ErrConnectionClosed) {
			return nil
		}
		return fmt.Errorf("nats: drain: %w", err)
	}

	<-c.closed
	return nil
}
