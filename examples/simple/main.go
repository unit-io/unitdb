package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	unite "github.com/unit-io/unite-go"
)

var f unite.MessageHandler = func(client unite.Client, msg unite.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func main() {
	client, err := unite.NewClient(
		"grpc://localhost:6061",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		unite.WithInsecure(),
		unite.WithKeepAlive(2*time.Second),
		unite.WithPingTimeout(1*time.Second),
		unite.WithDefaultMessageHandler(f),
	)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	ctx := context.Background()
	err = client.ConnectContext(ctx)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	r := client.Subscribe("teams.alpha.user1")
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("Hi #%d time!", i)
		r := client.Publish("teams.alpha.user1", msg)
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	wait := time.NewTicker(1 * time.Second)
	<-wait.C
	r = client.Unsubscribe("teams.alpha.user1")
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	wait.Stop()
	client.Disconnect()
}
