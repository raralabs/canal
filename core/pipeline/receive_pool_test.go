package pipeline

import (
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/raralabs/canal/core/message"
)

// This dummy struct mocks a Processor from the perspective of a receive pool in the next stage
type dummyProcessorForReceiver struct {
	sendChannel chan MsgPod
}

func (d *dummyProcessorForReceiver) Error(u uint8, err error) {
}
func (d *dummyProcessorForReceiver) Done() {
}
func (d *dummyProcessorForReceiver) addSendTo(stg *stage, route MsgRouteParam) {
	d.sendChannel = make(chan MsgPod, _SendBufferLength)
}
func (d *dummyProcessorForReceiver) channelForStageId(stg *stage) <-chan MsgPod {
	return d.sendChannel
}
func (d *dummyProcessorForReceiver) isConnected() bool {
	return d.sendChannel != nil
}

func (*dummyProcessorForReceiver) IsClosed() bool {
	return false
}

type dummyReceivePool struct {
	stg         *stage
	receiveFrom []IProcessorForReceiver
}

func newDummyReceivePool(s *stage) *dummyReceivePool {
	return &dummyReceivePool{
		stg: s,
	}
}

func (drp *dummyReceivePool) addReceiveFrom(processor IProcessorForReceiver) {
	drp.receiveFrom = append(drp.receiveFrom, processor)
}

func (drp *dummyReceivePool) lock() {
}

func (drp *dummyReceivePool) loop(pool IProcessorPool) {
	if len(drp.receiveFrom) > 0 {
		wg := sync.WaitGroup{}
		wg.Add(len(drp.receiveFrom))
		for _, proc := range drp.receiveFrom {
			rchan := proc.channelForStageId(drp.stg)
			go func() {
				for pod := range rchan {
					pool.execute(pod)
				}
				wg.Done()
			}()
		}
		wg.Wait()
	}
}

func (drp *dummyReceivePool) isRunning() bool {
	return true
}

func (drp *dummyReceivePool) error(code uint8, text string) {
}

// check if receive pool is properly instantiated
func TestReceivePool_newReceiverPool(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFact :=  newStageFactory(newPipeLine)
	stg := stgFact.new("firstStage",TRANSFORM)
	t.Run("newReceiverPool", func(t *testing.T) {
		recPool := newReceiverPool(stg)
		if !reflect.DeepEqual(recPool.stage, stg) {
			t.Errorf("Want: %v\nGot: %v\n", stg, recPool.stage)
		}
		assert.Equal(t,"[]pipeline.IProcessorForReceiver",reflect.TypeOf(recPool.receiveFrom).String(),
			"Datatype not as expected")
		assert.Equal(t,"chan<- message.Msg",reflect.TypeOf(recPool.errorSender).String())
	})
}


func TestReceiverPool_addReceiveFrom(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)
	stg := stgFactory.new("stage1",SOURCE)
	recStg := stgFactory.new("stage2",TRANSFORM)
	prcFactory := newProcessorFactory(stg)
	route := msgRoutes{"path": struct{}{}}
	processor1 :=  prcFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route)
	processor2 := prcFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route)
	recPool := newReceiverPool(recStg)
	t.Run("Test Receive From", func(t *testing.T) {
		assert.Equal(t,0,len(recPool.receiveFrom),"no processor added till now")
		recPool.addReceiveFrom(processor1)
		assert.Equal(t,1,len(recPool.receiveFrom),"only one procesor added")
		if !reflect.DeepEqual(processor1,recPool.receiveFrom[0]) {
			t.Errorf("Want: %v\nGot: %v\n", processor1, recPool.receiveFrom[0])
		}
		recPool.addReceiveFrom(processor2)
		assert.Equal(t,2,len(recPool.receiveFrom),"two procesors added")

		if !reflect.DeepEqual(processor2,recPool.receiveFrom[1]) {
			t.Errorf("Want: %v\nGot: %v\n", processor2, recPool.receiveFrom[1])
		}
		assert.Panics(t, func() {
			recPool.addReceiveFrom(processor2)
		},"duplicate processor added")
	})
}

func TestReceiverPool_lock(t *testing.T){
	newPipeLine := NewPipeline(uint32(1)) //created a new pipeline

	stgFactory := newStageFactory(newPipeLine) //factory to create the stages for newPipeLine


	//create sending stage of Source type and receiving stage of SinkType
	sndStage := stgFactory.new("sendStage",SOURCE)
	recStage := stgFactory.new("recStage",TRANSFORM)

	//creating routes for both stages
	routeForSrc := msgRoutes{"path1": struct{}{}}
	routeForSnk := msgRoutes{"path2": struct{}{}}

	sndProcFactory := newProcessorFactory(sndStage)
	sndProcessor := sndProcFactory.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),routeForSrc)
	sndProcessorPool := newProcessorPool(sndStage)
	sndProcessorPool.attach(sndProcessor)

	recProcFactory := newProcessorFactory(recStage)
	recProcessor := recProcFactory.new(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),routeForSnk)
	recProcessorPool := newProcessorPool(recStage)
	recReceiverPool := newReceiverPool(recStage)
	assert.Panics(t, func() {
		recReceiverPool.lock()
	},"Didn't panic for receiver pool doesn't have receive from")

	recReceiverPool.addReceiveFrom(sndProcessor)
	recProcessorPool.attach(recProcessor)

	assert.Panics(t, func() {
		recReceiverPool.lock()
	},"Receiver Pool without proper configuration should panic when lock is invoked")

	sndProcessor.addSendTo(recStage,"path2")
	assert.Equal(t,true,sndProcessor.isConnected())
}

func TestReceiverPool_loop(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	msgFactory := message.NewFactory(newPipeLine.id,1,1)
	msgContent := content2.New()
	msgContent.Add("greet",content2.NewFieldValue("hello",content2.STRING))
	msg := msgFactory.NewExecuteRoot(msgContent,false)
	packet := MsgPod{Msg:msg, Route:"path",}
	stgFactory := newStageFactory(newPipeLine)

	// Create a receiving stage
	rcvStage := stgFactory.new("rcvStage", TRANSFORM)
	rcvPool := rcvStage.receivePool
	// Create a processor that sends messages
	pr := &dummyProcessorForReceiver{}
	pr.addSendTo(rcvStage, "rcvPath")
	rcvPool.addReceiveFrom(pr)
	rcvPool.lock()

	// Create a dummy processor pool to intercept message sent by receiver
	dummyPP := newDummyProcessorPool("path2", nil)
	// Run receiver pool in a separate thread. It blocks till all the receiving channels are closed.
	go rcvPool.loop(dummyPP)

	//Send a message
	pr.sendChannel <- packet
	pr.sendChannel <- packet

	go func() {
		for {
			rcvd, ok := <-dummyPP.outRoute
			if !ok {
				break
			}
			m := rcvd.Msg
			if !reflect.DeepEqual(m.Content(), msg.Content()) {
				t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
			}
		}
	}()

	time.Sleep(10 * time.Millisecond)

	close(pr.sendChannel)



}

func TestReceivePool(t *testing.T) {

	t.Run("Simple Tests", func(t *testing.T) {

		pipelineId := uint32(1)
		//get the factory from message package which creates msg with given pipeline id and stageid,proccessor id
		msgF := message.NewFactory(pipelineId, 1, 1)
		content := content2.New()
		content =  content.Add("value", content2.NewFieldValue(12, content2.INT))

		msg := msgF.NewExecuteRoot(content, false)

		routeParam := MsgRouteParam("path1")
		msgPack := MsgPod{
			Msg:   msg,
			Route: routeParam,
		}

		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		// Create a stage that receives messages
		rcvStage := stgFactory.new("Second Node", TRANSFORM)
		rcvPool := rcvStage.receivePool

		// Create a processor that sends messages
		pr := &dummyProcessorForReceiver{}
		pr.addSendTo(rcvStage, routeParam)

		rcvPool.addReceiveFrom(pr)
		rcvPool.lock()

		// Create a dummy processor pool to intercept message sent by receiver
		dummyPP := newDummyProcessorPool("path2", nil)

		go rcvPool.loop(dummyPP)
		// Send a message
		pr.sendChannel <- msgPack
		pr.sendChannel <- msgPack

		go func() {
			for {
				rcvd, ok := <-dummyPP.outRoute
				if !ok {
					break
				}
				m := rcvd.Msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
			}
		}()

		time.Sleep(10 * time.Millisecond)

		assert.Equal(t,true,rcvPool.isRunning(),"loop runs the pool")
		close(pr.sendChannel)
	})


}

func BenchmarkReceivePool(b *testing.B) {

	b.Run("Simple Bench", func(b *testing.B) {
		b.ReportAllocs()

		pipelineId := uint32(1)

		msgF := message.NewFactory(pipelineId, 1, 1)
		content := content2.New()
		content = content.Add("value", content2.NewFieldValue(12, content2.INT))

		msg := msgF.NewExecuteRoot(content, false)

		routeParam := MsgRouteParam("path1")
		msgPack := MsgPod{
			Msg:   msg,
			Route: routeParam,
		}

		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		// Create a stage that sends messages
		sendStage := stgFactory.new("First Node", TRANSFORM)
		pr := &dummyProcessorForReceiver{}
		pr.addSendTo(sendStage, routeParam)

		// Create a stage that receives messages
		rcvStage := stgFactory.new("Second Node", TRANSFORM)
		rcvPool := newReceiverPool(rcvStage)
		rcvPool.addReceiveFrom(pr)
		rcvPool.lock()

		// Create a dummy processor pool to intercept message sent by receiver
		dummyPP := newDummyProcessorPool("path2", nil)

		for i := 0; i < b.N; i++ {
			// Run receiver pool in a separate thread. It blocks till all the receiving channels are closed.
			go rcvPool.loop(dummyPP)

			pr.sendChannel <- msgPack
			<-dummyPP.outRoute

		}
		close(pr.sendChannel)
	})

}
