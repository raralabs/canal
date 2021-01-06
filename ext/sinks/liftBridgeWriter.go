package sinks

import (
	"context"
	lift "github.com/liftbridge-io/go-liftbridge/v2"

	//"fmt"
	//"github.com/nats-io/nats.go"
	"github.com/raralabs/canal/core/pipeline"
	
)

//struct to hold the required attributes of the lift bridge writer
type LiftBridgeWriter struct {
	name       string              //name of the writer
	addrs 	   []string				//ports to publish the msg
	streamName string				//name of the stream of nats
	subject    string				//name of the subject of the nats from which the msg is published
									//for context with timeout
	client     *lift.Client
}


//initializes the liftbridge writer as per the given parameters
func NewLiftBridgeWriter(streamName,subject string ,LiftBridgeURL string) pipeline.Executor {
	var addrs []string
	var ctx context.Context
	ctx = context.Background()
	addrs = append(addrs,LiftBridgeURL)

	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}

	if err := client.CreateStream(ctx,subject,streamName); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
	return &LiftBridgeWriter{name: "liftBridgeWriter",addrs :addrs,subject: subject,streamName: streamName,client:&client}
}

func (lyft *LiftBridgeWriter) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}


func (lyft *LiftBridgeWriter) Execute(m pipeline.MsgPod, _ pipeline.IProcessorForExecutor) bool {
	//array of string to hold the ip and port
	//to which the connection is to be made

	client := *lyft.client

	if _, err := client.Publish(context.Background(), lyft.streamName, []byte(m.Msg.String())); err != nil {
		panic(err)
	}
	return false
}


func (lyft *LiftBridgeWriter) HasLocalState() bool {
	return false
}

func (lyft *LiftBridgeWriter) SetName(name string) {
	lyft.name = name
}

func (lyft *LiftBridgeWriter) Name() string {
	return lyft.name
}