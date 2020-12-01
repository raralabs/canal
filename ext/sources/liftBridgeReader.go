package sources

import (
	"context"
	"fmt"
	"github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"log"
	"time"
)

//data type for the options for the lift bridge subscriptions
type bridgeOpt uint8

const (
	NEW 		bridgeOpt = iota+1  //Subscribe starting with new message only
	RECENT							//Subscribe starting with most recent published value
	OLDEST							//Subscribe starting with oldest published value
	TIME
	OFFSET
	RANDOMISR
	AMTIME
)


type LiftBridgeReader struct{
	name 		string
	option 		bridgeOpt
	clientAddr  []string
	time 		time.Time //time for the subscription with TIME
	offset 		int64 	  // offset for the subscription with specific offset


}

//initializes new liftbridge subscription with the given name and option
func NewLiftBridgeReader(name string,option bridgeOpt,time time.Time,offset int64)*LiftBridgeReader{
	addrs := []string{"localhost:9292","localhost:9293","localhost:9294"}
	return &LiftBridgeReader{name:name,
		                     option:option,
		                     clientAddr:addrs,
		                     time:time,
		                     offset:offset,
	}
}

//executes the stage for the pipeline
func (lyft *LiftBridgeReader) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	client,err := liftbridge.Connect(lyft.clientAddr)
	if err != nil{
		log.Panic(err)
	}
	defer client.Close()
	newContent := content.New()
	ctx:= context.Background()
	switch lyft.option{
	case NEW:
		if err:=client.Subscribe(ctx, lyft.name, func(msg *liftbridge.Message, err error) {
			fmt.Println(msg.Offset(), string(msg.Value()))
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)

		});err!=nil{
			panic(err)
		}

	case OLDEST:
		if err := client.Subscribe(ctx, lyft.name, func(msg *liftbridge.Message, err error) {
			if err != nil {
				panic(err)
			}
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
			fmt.Println(msg.Offset(), string(msg.Value()))
		}, liftbridge.StartAtEarliestReceived()); err != nil {
			panic(err)
		}

	case RECENT:
		if err := client.Subscribe(ctx, lyft.name, func(msg *liftbridge.Message, err error) {
			if err != nil {
				panic(err)
			}
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
			fmt.Println(msg.Offset(), string(msg.Value()))
		}, liftbridge.StartAtLatestReceived()); err != nil {
			panic(err)
		}

	case TIME:
		if err := client.Subscribe(ctx, lyft.name, func(msg *liftbridge.Message, err error) {
			if err != nil {
				panic(err)
			}
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
			fmt.Println(msg.Offset(), string(msg.Value()))
		}, liftbridge.StartAtTime(time.Now())); err != nil {
			panic(err)
		}

	case OFFSET:
		if err := client.Subscribe(ctx,lyft.name,func(msg *liftbridge.Message,err error){
			if err != nil{
				panic(err)
			}
			fmt.Println(msg.Offset(),string(msg.Value()))
		},liftbridge.StartAtOffset(lyft.offset));err != nil{
			panic(err)
		}

	case RANDOMISR:
		if err := client.Subscribe(ctx,lyft.name,func(msg *liftbridge.Message,err error){
			if err != nil{
				panic(err)
			}
			fmt.Println(msg.Offset(),string(msg.Value()))
		},liftbridge.ReadISRReplica());err != nil{
			panic(err)
		}

	}
	<-ctx.Done()
	


	return false
}

//checks if the process is complete or not. If complete append eof to the message
func (lyft *LiftBridgeReader) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	contents := content.New()
	contents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, contents, nil)
	proc.Done()
}

//returns the source as executor type
func (lyft *LiftBridgeReader) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

//returns information of wether there are local states or not
func (lyft *LiftBridgeReader) HasLocalState() bool {
	return false
}

//sets the name of stage in the pipeline
func (lyft *LiftBridgeReader) SetName(name string) {
	lyft.name = name
}

func (lyft *LiftBridgeReader) Name() string {
	return lyft.name
}
