package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	unitdb "github.com/unit-io/unitdb-go"
)

var f unitdb.MessageHandler = func(client unitdb.Client, msg unitdb.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func main() {
	client, err := unitdb.NewClient(
		"grpc://localhost:6061",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		unitdb.WithInsecure(),
		unitdb.WithKeepAlive(2*time.Second),
		unitdb.WithPingTimeout(1*time.Second),
		unitdb.WithDefaultMessageHandler(f),
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
