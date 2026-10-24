package nats

import (
	"errors"
	"log"
	"time"

	natsgo "github.com/nats-io/nats.go"
)

type Config struct {
	URL  string
	Name string
}

type Client struct {
	conn *natsgo.Conn
}

func New(cfg Config) (*Client, error) {
	if cfg.URL == "" {
		cfg.URL = natsgo.DefaultURL
	}

	if cfg.Name == "" {
		return nil, errors.New("nats: client name cannot be empty")
	}

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
		}),
		natsgo.ConnectHandler(func(c *natsgo.Conn) {
			log.Printf("nats: connected to %s", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, err
	}

	return &Client{
		conn: conn,
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	c.conn.Close()
	return nil
}
