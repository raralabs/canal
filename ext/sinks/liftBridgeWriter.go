package sinks

import (
	"context"
	lift "github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/raralabs/canal/core/pipeline"
	"strconv"
)

//struct to hold the required attributes of the lift bridge writer
type LiftBridgeWriter struct {
	name       string              //name of the writer
	addrs 	   []string				//ports to publish the msg
	streamName string				//name of the stream of nats
	subject    string				//name of the subject of the nats from which the msg is published

}


//initializes the liftbridge writer as per the given parameters
func NewLiftBridgeWriter(streamName,subject string ,ports ...int64) pipeline.Executor {
	var addrs []string
	for _,port := range ports{
		prefixAddrs := "localhost:"
		fullAddr := prefixAddrs + strconv.FormatInt(port,10)
		addrs = append(addrs,fullAddr)

	}

	return &LiftBridgeWriter{name: "liftBridgeWriter",addrs :addrs,subject: subject,streamName: streamName}
}

func (lyft *LiftBridgeWriter) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}


func (lyft *LiftBridgeWriter) Execute(m pipeline.MsgPod, _ pipeline.IProcessorForExecutor) bool {
	//array of string to hold the ip and port
	//to which the connection is to be made

	ctx:= context.Background()
	//make a connection to the address
	client, err := lift.Connect(lyft.addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	//create a stream with given subject and name
	if err := client.CreateStream(ctx,lyft.subject, lyft.name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
	//write the published msg to the log
	if _, err := client.Publish(ctx, lyft.streamName, []byte(m.Msg.String())); err != nil {
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