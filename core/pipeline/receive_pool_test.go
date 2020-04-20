package pipeline

//
//import (
//	"github.com/raralabs/canal/core"
//	"testing"
//)
//
//func TestReceiverPool(t *testing.T) {
//
//	networkId := uint(1)
//	stageId := uint(1)
//
//	jf := newProcessorFactory(stageId)
//	task := NewPipeline(networkId)
//	hub := NewStage(stageId, task, TRANSFORM)
//
//	executor := core.newDummyExecutor()
//	transform1 := jf.new(hub, executor)
//	transform1.addSendTo(hub)
//
//	executor = core.newDummyExecutor()
//	transform2 := jf.new(hub, executor)
//	transform2.addSendTo(hub)
//
//	executor = core.newDummyExecutor()
//	transform3 := jf.new(hub, executor)
//	transform3.addSendTo(hub)
//
//	//msg := &Message{pipelineId: networkId, stageId: stageId, processorId: transform1.id,
//	//	mcontent: map[string]MessageAttr{"Greet": NewMsgValue("Moshi Moshi", STRING)}}
//	//
//	//rp := newReceiverPool(stageId)
//
//	//// add the jobs to the receivePool at first
//	//rp.addReceiveFrom(transform1)
//	//rp.addReceiveFrom(transform2)
//	//rp.addReceiveFrom(transform3)
//	//
//	//// Initialize the receivePool
//	//rp.initStage(true)
//	//
//	//// Pass the msg through the transforms so there is data at the
//	//// Sender sendChannel of the transforms
//	//transform1.process(msg)
//	//transform2.process(msg)
//	//transform3.process(msg)
//	//
//	//// loop the receivePool work
//	//count := 0
//	//numJobs := 3
//	//rp.loop(func(m *Message) bool {
//	//	count++
//	//	if !reflect.DeepEqual(msg, m) {
//	//		t.Errorf("Message: got = %#v, want = %#v", m, msg)
//	//	}
//	//	return count == numJobs
//	//})
//}
