package nats

import (
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

func (c *Client) Subscribe(
	subject string,
	handler func(data []byte),
) error {
	if c == nil {
		return errors.New("nats: client is nil")
	}

	if c.conn == nil {
		return errors.New("nats: connection is nil")
	}

	if subject == "" {
		return errors.New("nats: subject cannot be empty")
	}

	if handler == nil {
		return errors.New("nats: handler cannot be nil")
	}

	if c.conn.IsClosed() {
		return errors.New("nats: connection is closed")
	}

	_, err := c.conn.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})

	if err != nil {
		return fmt.Errorf("nats: subscribe to subject %q: %w", subject, err)
	}

	return nil

}
