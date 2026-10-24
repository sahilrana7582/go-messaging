package nats

import (
	"errors"
	"fmt"
)

func (c *Client) Publish(subject string, data []byte) error {
	if c == nil {
		return errors.New("nats: client is nil")
	}

	if c.conn == nil {
		return errors.New("nats: connection is nil")
	}

	if subject == "" {
		return errors.New("nats: subject cannot be empty")
	}

	if len(data) == 0 {
		return errors.New("nats: message data cannot be empty")
	}

	if err := c.conn.Publish(subject, data); err != nil {
		return fmt.Errorf("nats: publish to subject %q: %w", subject, err)
	}

	return nil
}
