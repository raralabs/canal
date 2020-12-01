package sources

import (
	"context"
	"fmt"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/raralabs/canal/core/pipeline"
	"testing"
	"time"
)

func TestNewLiftBridgeReader(t *testing.T) {
	addrs := []string{"localhost:9292","localhost:4222","localhost:6222"}
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
	reader := NewLiftBridgeReader("reader",RECENT,time.Now(),int64(1))
	var m pipeline.MsgPod
	var proc pipeline.IProcessorForExecutor
	flag := reader.Execute(m,proc)
	fmt.Println(flag)
}
