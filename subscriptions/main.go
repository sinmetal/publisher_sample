package main

import (
	"fmt"
	"log"
	"time"

	"golang.org/x/net/context"

	"cloud.google.com/go/pubsub"
)

func main() {
	ctx := context.Background()

	client, err := pubsub.NewClient(ctx, "sinmetal-pubsub")
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
	}

	subscription := "mysubscription"

	for {
		log.Println("pull message")
		err = pullMsgs(ctx, client, subscription)
		if err != nil {
			log.Fatalf("Failed Pull Message. subscription = %s : %v", subscription, err)
		}
	}
}

func pullMsgs(ctx context.Context, client *pubsub.Client, subscriptionName string) error {
	sub := client.Subscription(subscriptionName)

	cctx, cancel := context.WithCancel(ctx)

	err := sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		err := work(msg)
		if err != nil {
			log.Printf("Failed Work Message. %v", err)
			cancel()
			msg.Nack()
			return
		}

		msg.Ack()
	})

	return err
}

func work(msg *pubsub.Message) error {
	if (time.Now().Nanosecond() % 12) == 0 {
		// サンプルのために適当にエラーを起こす
		return fmt.Errorf("dummy error: message: %s %q %s", msg.ID, string(msg.Data), msg.PublishTime)
	}

	fmt.Printf("Got message: %s %q %s\n", msg.ID, string(msg.Data), msg.PublishTime)
	return nil
}
