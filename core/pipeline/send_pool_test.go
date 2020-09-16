package pipeline

import (
	"github.com/raralabs/canal/core/message"
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestSendPool_newSendPool(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)
	stg :=  stgFactory.new("stage1",SOURCE)
	prFactory := newProcessorFactory(stg)
	route := msgRoutes{"path": struct{}{}}
	processor1 := prFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route)
	t.Run("Create Send Pool", func(t *testing.T) {
		sndPool := newSendPool(processor1)
		if !reflect.DeepEqual(sndPool.proc,processor1) {
			t.Errorf("Want: %v\nGot: %v\n", processor1,sndPool.proc)
		}
		assert.Equal(t,"chan<- message.Msg",reflect.TypeOf(sndPool.errSender).String(),"data type mismatch")
		assert.Equal(t,"map[*pipeline.stage]pipeline.sendRoute",reflect.TypeOf(sndPool.sndRoutes).String(),"datatype didn't match")
	})
}



//Tests
//- isConnected()
//- addSendTO
// -getChannel

func TestSendPool(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)
	sendingStg :=  stgFactory.new("stage1",TRANSFORM)
	receivingStg := stgFactory.new("stage2",SINK)
	route := msgRoutes{"path": struct{}{}}
	sendingProcessorPool := newProcessorPool(sendingStg)
	SendingProcessorFactory := newProcessorFactory(sendingStg)
	sendingProcessor := SendingProcessorFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route)
	sendingProcessorPool.attach(sendingProcessor)
	sendingStg.processorPool = sendingProcessorPool
	assert.Equal(t,false,sendingProcessor.sndPool.isConnected(),
		"send pool connected without invoking addSendTo method")
	sendingProcessor.sndPool.addSendTo(receivingStg,"pathForReceiver")
	assert.Equal(t,1,len(sendingProcessor.sndPool.sndRoutes),"routes added but not shown")
	assert.Equal(t,true,sendingProcessor.sndPool.isConnected(),
		"processor connected but send pool failed to connect")
	t.Run("Simple Processor Pool Test", func(t *testing.T) {
		// Generate Message
		//msgF := message.NewFactory(newPipeLine.id, sendingStg.id, sendingProcessor.id)
		content := content2.New()
		content = content.Add("value", content2.NewFieldValue(12, content2.INT))

		//msg := msgF.NewExecuteRoot(content, false)
		channels := sendingProcessor.sndPool.getChannel(receivingStg)
		assert.Equal(t, "<-chan pipeline.msgPod", reflect.TypeOf(channels).String())
	//
	//	t.Run("Test1", func(t *testing.T) {
	//		msgPack := msgPod{
	//			msg:   msg,
	//			route: "path",
	//		}
	//		sendingStg.processorPool.lock(route)
	//		sendingProcessorPool.execute(msgPack)
	//
	//		select {
	//		case rcvd := <-channels:
	//			m := rcvd.msg
	//			if !reflect.DeepEqual(m.Content(), msg.Content()) {
	//				t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
	//			}
	//			assert.Equal(t, msg.Content(), m.Content())
	//		}
	//	})
	})
}

func TestSendPool_sndMessage(t *testing.T){
	msgFactory := message.NewFactory(1,1,1)
	msgContent := content2.New()
	msgContent.Add("greet",content2.NewFieldValue("hello",content2.STRING))
	msg := msgFactory.NewExecuteRoot(msgContent,false)
	pipeLne := NewPipeline(uint32(1))
	stgFact := newStageFactory(pipeLne)
	stg := stgFact.new("stg",SOURCE)
	stg2 := stgFact.new("stg2",TRANSFORM)
	procPool := newProcessorPool(stg)
	route:= msgRoutes{"path": struct{}{}}
	procFactory := newProcessorFactory(stg)
	processor1 := procFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route)
	procPool.attach(processor1)
	processor1.addSendTo(stg2,"path2")
	sent := processor1.sndPool.send(msg,true)
	assert.Equal(t,false,sent,"sent without locking the snd pool")
	processor1.sndPool.lock()
	sent = processor1.sndPool.send(msg,true)
	assert.Equal(t,true,sent,"could not send the msg")
}

func TestSendPool_operations(t *testing.T){
	pipeLne := NewPipeline(uint32(2))
	stgFact := newStageFactory(pipeLne)
	stg := stgFact.new("stg",SOURCE)
	procPool := newProcessorPool(stg)
	route:= msgRoutes{"path": struct{}{}}
	procFactory := newProcessorFactory(stg)
	processor1 := procFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route)
	procPool.attach(processor1)
	t.Run("error", func(t *testing.T) {
		assert.Equal(t,false,processor1.sndPool.isLocked(),"snd pool not locked yet")
		processor1.sndPool.error(1,"error1")
		processor1.sndPool.lock()
		assert.Equal(t,true,processor1.sndPool.isLocked(),"snd pool not locked yet")
		assert.Equal(t,false,processor1.sndPool.isClosed(),"snd pool not closed yet")
		processor1.sndPool.close()
		assert.Equal(t,true,processor1.sndPool.isClosed(),"snd pool not closed yet")

	})

}



