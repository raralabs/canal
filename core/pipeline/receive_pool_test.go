package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"reflect"
	"testing"
)

// This dummy struct mocks a Processor from the perspective of a receive pool in the next stage
type dummyProcessorForReceiver struct {
	sendChannel chan msgPod
}

func (d *dummyProcessorForReceiver) Error(u uint8, err error) {
}
func (d *dummyProcessorForReceiver) Done() {
}
func (d *dummyProcessorForReceiver) addSendTo(stg *stage, route msgRouteParam) {
	d.sendChannel = make(chan msgPod, _SendBufferLength)
}
func (d *dummyProcessorForReceiver) channelForStageId(stg *stage) <-chan msgPod {
	return d.sendChannel
}
func (d *dummyProcessorForReceiver) isConnected() bool {
	return d.sendChannel != nil
}

// This dummy struct mocks a Processor Pool from the perspective of a receive pool. So we will only implement following:
// execute(), done()
type dummyProcessorPool struct {
	outRoute chan msgPod
}

func newDummyProcessorPool(route msgRouteParam) *dummyProcessorPool {
	sendChannel := make(chan msgPod, _SendBufferLength)
	return &dummyProcessorPool{
		outRoute: sendChannel,
	}
}
func (d *dummyProcessorPool) add(exec Executor, routes msgRoutes) IProcessor {
	return nil
}
func (d *dummyProcessorPool) shortCircuitProcessors() {
}
func (d *dummyProcessorPool) lock(stgRoutes msgRoutes) {
}
func (d *dummyProcessorPool) isClosed() bool {
	return false
}
func (d *dummyProcessorPool) isRunning() bool {
	return true
}
func (d *dummyProcessorPool) stage() *stage {
	return nil
}
func (d *dummyProcessorPool) attach(pool ...IProcessorForPool) {
}
func (d *dummyProcessorPool) detach(pool ...IProcessorForPool) {
}
func (d *dummyProcessorPool) execute(pod msgPod) {
	d.outRoute <- pod
}
func (d *dummyProcessorPool) error(u uint8, err error) {
}
func (d *dummyProcessorPool) done() {
}

func TestReceivePool(t *testing.T) {

	t.Run("Simple Tests", func(t *testing.T) {

		pipelineId := uint32(1)

		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		routeParam := msgRouteParam("path1")
		msgPack := msgPod{
			msg:   msg,
			route: routeParam,
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
		dummyPP := newDummyProcessorPool("path2")

		// Run receiver pool in a separate thread. It blocks till all the receiving channels are closed.
		go rcvPool.loop(dummyPP)

		// Send a message
		pr.sendChannel <- msgPack

		select {
		case rcvd := <-dummyPP.outRoute:
			m := rcvd.msg
			if !reflect.DeepEqual(m.Content(), msg.Content()) {
				t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
			}
		}

		close(pr.sendChannel)
		close(dummyPP.outRoute)
	})

}

func BenchmarkReceivePool(b *testing.B) {

	b.Run("Simple Bench", func(b *testing.B) {
		b.ReportAllocs()

		pipelineId := uint32(1)

		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		routeParam := msgRouteParam("path1")
		msgPack := msgPod{
			msg:   msg,
			route: routeParam,
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
		dummyPP := newDummyProcessorPool("path2")

		for i := 0; i < b.N; i++ {
			// Run receiver pool in a separate thread. It blocks till all the receiving channels are closed.
			go rcvPool.loop(dummyPP)

			pr.sendChannel <- msgPack
			<-dummyPP.outRoute

		}
		close(pr.sendChannel)
		close(dummyPP.outRoute)
	})

}
