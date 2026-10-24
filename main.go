package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/nats-io/nats.go"
	messaging "github.com/sahilrana7582/go-messaging/nats"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found; using environment variables")
	}

	client, err := messaging.New(messaging.Config{
		URL:  os.Getenv("NATS_URL"),
		Name: "taskflow",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	sub, err := client.Subscribe("orders.*", func(msg *nats.Msg) {
		fmt.Printf("Received on %s: %s\n", msg.Subject, string(msg.Data))
		wg.Done()
	})
	if err != nil {
		log.Fatalf("failed to subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	log.Println("NATS client initialized successfully")

	time.Sleep(100 * time.Millisecond)

	err = client.Publish(
		"orders.created",
		[]byte(`{"order_id":123}`),
	)
	if err != nil {
		log.Printf("failed to publish message: %v", err)
		return
	}
	wg.Wait()
}
