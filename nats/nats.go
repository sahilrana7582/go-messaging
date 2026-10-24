package nats

import (
	"log"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var (
	natsServerConn *NatsServer
	natsOnce       sync.Once
)

type NatsServer struct {
	nats *nats.Conn

	js     nats.JetStreamContext
	jsOnce sync.Once
	jsErr  error
}

func initialize(url, clientName string) (*NatsServer, error) {
	natsConn, err := nats.Connect(url,
		nats.Name(clientName),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(3*time.Second),
		nats.ReconnectJitter(500*time.Millisecond, 2*time.Second),
		nats.RetryOnFailedConnect(true),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			log.Printf("nats: disconnected, reconnecting: %v", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			log.Printf("nats: reconnected to %s", c.ConnectedUrl())
		}),
		nats.ClosedHandler(func(c *nats.Conn) {
			log.Printf("nats: connection closed: %v", c.LastError())
		}),
		nats.ConnectHandler(func(c *nats.Conn) {
			log.Printf("Trying to connect for: Url: %q, Client: %q", url, clientName)
		}),
	)

	if err != nil {
		return nil, err
	}

	return &NatsServer{nats: natsConn}, nil
}

func GetNatsServer(clientName string) *NatsServer {
	natsOnce.Do(func() {
		natsURL := os.Getenv("NATS_URL")
		if natsURL == "" {
			natsURL = nats.DefaultURL
		}

		server, err := initialize(natsURL, clientName)
		if err != nil {
			panic(err)
		}

		natsServerConn = server
	})

	return natsServerConn
}
