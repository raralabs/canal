package main

import (
	"fmt"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/raralabs/canal/core/message/content"
	"golang.org/x/net/context"
)

func main() {
	// Create Liftbridge client.
	addrs := []string{"localhost:9292"}

	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Create a stream attached to the NATS subject "foo".
	var (
		subject = "foo"
		name    = "foo-stream"
	)

	if err := client.CreateStream(context.Background(), subject, name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}

	// Publish a message to "foo".

		for i:=0;i<2;i++{
			if _, err := client.Publish(context.Background(), name, []byte("eterce")); err != nil {
				panic(err)
			}
		}



	// Subscribe to the stream starting from the beginning.
	ctx := context.Background()
	if err := client.Subscribe(ctx, "foo-stream", func(msg *lift.Message, err error) {
		if err != nil {
			panic(err)
		}
		cnt := content.New()
		cnt.Add("data",content.NewFieldValue(string(msg.Value()),content.STRING))
		fmt.Println(cnt)

	}, lift.StartAtEarliestReceived()); err != nil {
		panic(err)
	}
	<-ctx.Done()
}
