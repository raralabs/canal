package sources

import (
	"context"
	"github.com/liftbridge-io/go-liftbridge/v2"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"log"
	"strconv"
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
	name 		string	  //name of the sink
	streamName 	string    //name of the lift bridge stream you want to connect to
	option 		bridgeOpt //options for the type of read
	clientAddr  []string  //holds the ip address and port to which the connection is to be made
	time 		time.Time //time for the subscription with TIME
	offset 		int64 	  // offset for the subscription with specific offset


}

//initializes new liftbridge subscription with the given name and option
func NewLiftBridgeReader(name string,option bridgeOpt,time time.Time,offset int64,ip string,ports ...int)*LiftBridgeReader{
	var addrs []string
	for _, port := range ports{
		fullAddr := ip + strconv.Itoa(port)
		addrs = append(addrs,fullAddr)
	}
	return &LiftBridgeReader{streamName:name,
		                     option:option,
		                     clientAddr:addrs,
		                     time:time,
		                     offset:offset,
	}
}

//executes the stage for the pipeline
func (lyft *LiftBridgeReader) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	//make connection to the port specified for getting the msg
	client,err := liftbridge.Connect(lyft.clientAddr)
	//if connections can't be made log error
	if err != nil{
		log.Panic(err)
	}
	defer client.Close()
	//create a stream called pgservice-stream
	//if err := client.CreateStream(context.Background(), "event.*","event-stream"); err != liftbridge.ErrStreamExists && err != nil {
	//	panic(err)
	//}
	ctx:= context.Background()
	//cases for subscribing to the data
	//lift-bridge supports total 7 options for subscribing the data

	switch lyft.option{

	case NEW:
		if err:=client.Subscribe(ctx, lyft.streamName, func(msg *liftbridge.Message, err error) {
			newContent := content.New()
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
		});err!=nil{
			panic(err)
		}


	case OLDEST:
		if err := client.Subscribe(ctx, lyft.streamName, func(msg *liftbridge.Message, err error) {
			if err != nil {
				panic(err)
			}
			newContent := content.New()
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
		}, liftbridge.StartAtEarliestReceived()); err != nil {
			panic(err)
		}

	case RECENT:
		if err := client.Subscribe(ctx, lyft.streamName, func(msg *liftbridge.Message, err error) {
			if err != nil {
				panic(err)
			}
			newContent := content.New()
			newContent.Add("msg",content.NewFieldValue(msg.Value(), content.BYTE))
			newContent.Add("subject",content.NewFieldValue(msg.Subject(),content.STRING))
			proc.Result(m.Msg, newContent, nil)
		}, liftbridge.StartAtLatestReceived()); err != nil {
			panic(err)
		}

	case TIME:
		if err := client.Subscribe(ctx, lyft.streamName, func(msg *liftbridge.Message, err error) {
			if err != nil {
				panic(err)
			}
			newContent := content.New()
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
		}, liftbridge.StartAtTime(time.Now())); err != nil {
			panic(err)
		}

	case OFFSET:
		if err := client.Subscribe(ctx,lyft.streamName,func(msg *liftbridge.Message,err error){
			if err != nil{
				panic(err)
			}
			newContent := content.New()
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
		},liftbridge.StartAtOffset(lyft.offset));err != nil{
			panic(err)
		}

	case RANDOMISR:
		if err := client.Subscribe(ctx,lyft.streamName,func(msg *liftbridge.Message,err error){
			if err != nil{
				panic(err)
			}
			newContent := content.New()
			newContent.Add("msg",content.NewFieldValue(string(msg.Value()), content.STRING))
			proc.Result(m.Msg, newContent, nil)
		},liftbridge.ReadISRReplica());err != nil{
			panic(err)
		}

	}
	<-ctx.Done()
	 lyft.done(m.Msg, proc)
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

//returns information of whether there are local states or not
func (lyft *LiftBridgeReader) HasLocalState() bool {
	return false
}

//sets the name of stage in the pipeline
func (lyft *LiftBridgeReader) SetName(name string) {
	lyft.name = name
}

//returns the name of the lyftbridge
func (lyft *LiftBridgeReader) Name() string {
	return lyft.name
}
