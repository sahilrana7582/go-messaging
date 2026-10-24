package main

import (
	"github.com/joho/godotenv"
	"github.com/sahilrana7582/go-messaging/nats"
)

func main() {
	godotenv.Load()
	natsConn := nats.GetNatsServer("Dev: Main")

}
