package main

import (
	"fmt"
	"log"

	"golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, "sinmetal-pubsub")
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}

	topic := "mytopic"
	msg := "Hello Cloud Pub/Sub"
	err = publish(ctx, client, topic, msg)
	if err != nil {
		log.Fatalf("Failed Publish. topic = %s, message = %s : %v", topic, msg, err)
	}
}

func publish(ctx context.Context, client *pubsub.Client, topic, msg string) error {
	t := client.Topic(topic)
	result := t.Publish(ctx, &pubsub.Message{
		Data: []byte(msg),
	})

	id, err := result.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("Published a message; msg ID: %v\n", id)

	return nil
}
