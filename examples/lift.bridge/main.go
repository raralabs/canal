package main

import (
	"encoding/json"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
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
		subject = "pgservice.write"
		name    = "pg"
	)

	if err := client.CreateStream(context.Background(), subject, name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}

	// Publish a message to "foo".
	msg:=map[string]interface{}{"request_id":"37f90977-5c24-4832-a687-510c3aaa70c1","source_data":"bnVsbA==","auth_identity":"",
		"content":map[string]interface{}{"namespace":"rara","collection":"Lead","success":true,"error":"","id":"8e7cb5d3-8596-41aa-99fb-729b1fd2272c",
			"before":map[string]interface{}{"status":"Open",
				"remind_to":[]string{"90f41c84-7b0a-47f5-95aa-6cbec990506e"},
			"after":map[string]interface{}{"CreatedBy":"","status":"Open","collection":"Contacts",
				"remind_to":[]string{"90f41c84-7b0a-47f5-95aa-6cbec990506e","437adeff-d56f-46b6-b664-a36e1cc42dc4","c249e3a2-a5b2-4f6e-99b9-ca952fddc6c9","aa164c86-05c4-4d22-8b12-a16bad5af626"}
				}}}

	bytemsg,err:=json.Marshal(msg)
	if _, err := client.Publish(context.Background(), name, (bytemsg)); err != nil {
		panic(err)
	}

	// Subscribe to the stream starting from the beginning.
	//ctx := context.Background()
	//if err := client.Subscribe(ctx, "foo-stream", func(msg *lift.Message, err error) {
	//	if err != nil {
	//		panic(err)
	//	}
	//	cnt := content.New()
	//	cnt.Add("data",content.NewFieldValue(string(msg.Value()),content.STRING))
	//	fmt.Println(cnt)
	//
	//}, lift.StartAtEarliestReceived()); err != nil {
	//	panic(err)
	//}
	//<-ctx.Done()
}
