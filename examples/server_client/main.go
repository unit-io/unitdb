package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	udb "github.com/unit-io/unitdb-go"
)

func main() {
	recv := make(chan [2][]byte)

	client, err := udb.NewClient(
		"grpc://localhost:6080",
		"UCBFDONCNJLaKMCAIeJBaOVfbAXUZHNPLDKKLDKLHZHKYIZLCDPQ",
		udb.WithInsecure(),
		udb.WithConnectionLostHandler(func(client udb.Client, err error) {
			if err != nil {
				log.Fatal(err)
			}
			close(recv)
		}),
		udb.WithDefaultMessageHandler(func(client udb.Client, pubMsg udb.PubMessage) {
			for _, msg := range pubMsg.Messages() {
				recv <- [2][]byte{[]byte(msg.Topic), msg.Payload}
			}
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

	var r udb.Result

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Hi sales channel #%d time!", i)
		r = client.Publish("teams.private.sales.saleschannel.message", []byte(msg), udb.WithTTL("1m"), udb.WithPubDeliveryMode(1))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Hi customer connect channel #%d time!", i)
		r = client.Publish("teams.public.customersupport.connectchannel.message", []byte(msg), udb.WithTTL("1m"), udb.WithPubDeliveryMode(1))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	for i := 0; i < 3; i++ {
		msg := fmt.Sprintf("Hi all sales channels #%d time!", i)
		r = client.Publish("teams.private.sales.*.message", []byte(msg), udb.WithTTL("1m"), udb.WithPubDeliveryMode(1))
		if _, err := r.Get(ctx, 1*time.Second); err != nil {
			log.Fatalf("err: %s", err)
		}
	}

	r = client.Relay("teams.private.sales.saleschannel.message", udb.WithLast("1m"))
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
