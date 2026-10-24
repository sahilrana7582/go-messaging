package nats

import (
	"errors"
	"fmt"
	"log"
	"runtime/debug"

	natsgo "github.com/nats-io/nats.go"
)

type Handler func(msg *natsgo.Msg)

type Subscription struct {
	sub *natsgo.Subscription
}

func (c *Client) Subscribe(
	subject string,
	handler Handler,
) (*Subscription, error) {
	if err := c.validateSubscription(subject, handler); err != nil {
		return nil, err
	}

	sub, err := c.conn.Subscribe(subject, func(msg *natsgo.Msg) {
		safeHandler(msg, handler)
	})
	if err != nil {
		return nil, fmt.Errorf("nats: subscribe to subject %q: %w", subject, err)
	}

	return &Subscription{sub: sub}, nil
}

func (c *Client) QueueSubscribe(
	subject string,
	queueName string,
	handler Handler,
) (*Subscription, error) {
	if err := c.validateSubscription(subject, handler); err != nil {
		return nil, err
	}

	if queueName == "" {
		return nil, errors.New("nats: queue name cannot be empty")
	}

	sub, err := c.conn.QueueSubscribe(subject, queueName, func(msg *natsgo.Msg) {
		safeHandler(msg, handler)
	})

	if err != nil {
		return nil, fmt.Errorf(
			"nats: queue subscribe to subject %q with queue %q: %w",
			subject,
			queueName,
			err,
		)
	}

	return &Subscription{sub: sub}, nil
}

func (s *Subscription) Unsubscribe() error {
	if s == nil || s.sub == nil {
		return nil
	}

	if err := s.sub.Unsubscribe(); err != nil {
		return fmt.Errorf("nats: unsubscribe from subject %q: %w", s.sub.Subject, err)
	}

	return nil
}

func (c *Client) validateSubscription(
	subject string,
	handler Handler,
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

	return nil
}

func safeHandler(msg *natsgo.Msg, handler Handler) {
	defer func() {
		if r := recover(); r != nil {
			logHandlerPanic(msg, r)
		}
	}()

	handler(msg)
}

func logHandlerPanic(msg *natsgo.Msg, recovered any) {
	subject := ""
	if msg != nil {
		subject = msg.Subject
	}

	log.Printf(
		"nats: recovered from message handler panic | subject=%q | panic=%v\nStack trace:\n%s",
		subject,
		recovered,
		debug.Stack(),
	)
}
