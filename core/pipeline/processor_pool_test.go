package pipeline

//
//import (
//	"github.com/n-is/canal/core"
//	"testing"
//)
//
//func TestSendPool(t *testing.T) {
//	pipelineId := uint(1)
//	stageId := uint(1)
//
//	pipeline := NewPipeline(pipelineId)
//	stage := NewStage(stageId, pipeline, TRANSFORM)
//
//	sp := newProcessorPool(pipeline, stage.Id)
//
//	// add the jobs to the sendpool at first
//	sp.add(core.newDummyExecutor())
//	sp.add(core.newDummyExecutor())
//	sp.add(core.newDummyExecutor())
//
//	// Initialize the sendpool
//	sp.lock()
//
//	// process the sendpool
//	//sp.process(msg)
//	//
//	////m := <-transform1.Sender
//	////if !reflect.DeepEqual(msg, m) {
//	////	t.Errorf("Message: got = %#v, want = %#v", m, msg)
//	////}
//	////
//	////m = <-transform2.Sender
//	////if !reflect.DeepEqual(msg, m) {
//	////	t.Errorf("Message: got = %#v, want = %#v", m, msg)
//	////}
//	////
//	////m = <-transform3.Sender
//	////if !reflect.DeepEqual(msg, m) {
//	////	t.Errorf("Message: got = %#v, want = %#v", m, msg)
//	////}
//	//
//	//// Or we could have simply added a receivepool to connect and obtain
//	//// messages sent using sendpool
//	//rp := newReceiverPool(stageId)
//	//
//	//// add the jobs to the receiverpool at first
//	//rp.addReceiveFrom(transform1)
//	//rp.addReceiveFrom(transform2)
//	//rp.addReceiveFrom(transform3)
//	//
//	//// Initialize the receivePool
//	//rp.initStage(true)
//	//
//	//// process the sendPool
//	//sp.process(msg)
//	//
//	//// loop the receivePool work
//	//count := 0
//	//numJobs := 3
//	//
//	//rp.loop(func(m *Message) bool {
//	//	count++
//	//	if !reflect.DeepEqual(msg, m) {
//	//		t.Errorf("Message: got = %#v, want = %#v", m, msg)
//	//	}
//	//	return count == numJobs
//	//})
//}
