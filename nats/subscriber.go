package nats

import (
	"errors"
	"fmt"
	"runtime/debug"

	natsgo "github.com/nats-io/nats.go"
)

func (c *Client) Subscribe(
	subject string,
	handler func(data []byte),
) error {
	if err := c.validateSubscription(subject, handler); err != nil {
		return err
	}

	_, err := c.conn.Subscribe(subject, func(msg *natsgo.Msg) {
		safeHandler(msg, handler)
	})
	if err != nil {
		return fmt.Errorf("nats: subscribe to subject %q: %w", subject, err)
	}

	return nil
}

func (c *Client) QueueSubscribe(
	subject string,
	queueName string,
	handler func(data []byte),
) error {
	if err := c.validateSubscription(subject, handler); err != nil {
		return err
	}

	if queueName == "" {
		return errors.New("nats: queue name cannot be empty")
	}

	_, err := c.conn.QueueSubscribe(subject, queueName, func(msg *natsgo.Msg) {
		safeHandler(msg, handler)
	})

	if err != nil {
		return fmt.Errorf(
			"nats: queue subscribe to subject %q with queue %q: %w",
			subject,
			queueName,
			err,
		)
	}

	return nil
}

func (c *Client) validateSubscription(
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
		return errors.New("nats: handler must not be nil")
	}

	if c.conn.IsClosed() {
		return errors.New("nats: connection is closed")
	}

	return nil
}

func safeHandler(msg *natsgo.Msg, handler func(data []byte)) {
	defer func() {
		if r := recover(); r != nil {
			logHandlerPanic(msg, r)
		}
	}()

	handler(msg.Data)
}

func logHandlerPanic(msg *natsgo.Msg, recovered any) {
	subject := ""
	if msg != nil {
		subject = msg.Subject
	}

	fmt.Printf(
		"NATS: recovered from message handler panic | subject=%q | panic=%v\nStack trace:\n%s",
		subject,
		recovered,
		debug.Stack(),
	)
}
