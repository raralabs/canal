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
	ports 	   []string				//ports to publish the msg
	streamName string				//name of the stream of nats
	subject    string				//name of the subject of the nats from which the msg is published

}


//initializes the liftbridge writer as per the given parameters
func NewLiftBridgeWriter(streamName,subject string ,ports ...int64) pipeline.Executor {
	var connectPorts  []string
	for _,port := range ports{
		connectPorts = append(connectPorts,strconv.FormatInt(port, 10))
	}

	return &LiftBridgeWriter{name: "liftBridgeWriter",ports :connectPorts,subject: subject,streamName: streamName}
}

func (lyft *LiftBridgeWriter) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

//old implementation of the code
//func (s *BlackholeSink) Execute(m message.Msg, pr pipeline.IProcessorForExecutor) bool {
//	return true
//}

func (lyft *LiftBridgeWriter) Execute(m pipeline.MsgPod, _ pipeline.IProcessorForExecutor) bool {
	addrs := []string{}
	ctx:= context.Background()
	for _,port := range lyft.ports{
		prefixAddrs := "localhost:"
		fullAddr := prefixAddrs + port
		addrs = append(addrs,fullAddr)
	}
	client, err := lift.Connect(addrs)
	if err != nil {
		panic(err)
	}
	defer client.Close()
	if err := client.CreateStream(ctx,lyft.subject, lyft.name); err != nil {
		if err != lift.ErrStreamExists {
			panic(err)
		}
	}
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