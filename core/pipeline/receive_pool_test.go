package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"reflect"
	"sync"
	"testing"
	"time"
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

		// Run receiver pool in a separate thread. It blocks till all the receiving channels are closed.
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
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
			}
		}()

		time.Sleep(10*time.Millisecond)

		close(pr.sendChannel)
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
