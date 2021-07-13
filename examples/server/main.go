package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	unitdb "github.com/unit-io/unitdb-go"
)

func main() {
	recv := make(chan [2][]byte)

	client, err := unitdb.NewClient(
		"grpc://localhost:6080",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		unitdb.WithInsecure(),
		unitdb.WithConnectionLostHandler(func(client unitdb.Client, err error) {
			if err != nil {
				log.Fatal(err)
			}
			close(recv)
		}),
		unitdb.WithDefaultMessageHandler(func(client unitdb.Client, msg unitdb.Message) {
			recv <- [2][]byte{[]byte(msg.Topic()), msg.Payload()}
		}),
	)
	if err != nil {
		log.Fatalf("err: %s", err)
	}
	ctx := context.Background()
	err = client.ConnectContext(ctx)
	if err != nil {
		log.Fatalf("err: %s", err)
	}

	var r unitdb.Result

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Hi mygroup1 #%d time!", i)
		r = client.Publish("groups.private.mygroup1.message", []byte(msg), unitdb.WithTTL("1m"), unitdb.WithPubDeliveryMode(1))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Hi mygroup2 #%d time!", i)
		r = client.Publish("groups.private.mygroup2.message", []byte(msg), unitdb.WithTTL("1m"), unitdb.WithPubDeliveryMode(1))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Hi all private groups #%d time!", i)
		r = client.Publish("groups.private.*.message", []byte(msg), unitdb.WithTTL("1m"), unitdb.WithPubDeliveryMode(1))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	r = client.Relay("groups.private.mygroup1.message", unitdb.WithLast("1m"))
	if _, err := r.Get(ctx, 1*time.Second); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for {
		select {
		case <-ctx.Done():
			client.DisconnectContext(ctx)
			fmt.Println("Client Disconnected")
			return
		case incoming := <-recv:
			fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		}
	}
}
