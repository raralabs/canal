package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"testing"
	"time"
)

// This dummy struct mocks a Processor Pool from the perspective of a receive pool. So we will only implement following:
// execute(), done()
type dummyProcessorPool struct {
	routeMu    *sync.Mutex
	outRoute   chan msgPod
	chanClosed bool
	stg        *stage
}

func newDummyProcessorPool(route msgRouteParam, stg *stage) *dummyProcessorPool {
	sendChannel := make(chan msgPod, _SendBufferLength)
	return &dummyProcessorPool{
		outRoute:   sendChannel,
		chanClosed: false,
		routeMu:    &sync.Mutex{},
		stg:        stg,
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
	d.routeMu.Lock()
	defer d.routeMu.Unlock()

	return d.chanClosed
}
func (d *dummyProcessorPool) isRunning() bool {
	d.routeMu.Lock()
	defer d.routeMu.Unlock()

	return !d.chanClosed
}
func (d *dummyProcessorPool) stage() *stage {
	return d.stg
}
func (d *dummyProcessorPool) attach(pool ...IProcessorForPool) {
}
func (d *dummyProcessorPool) detach(pool ...IProcessorForPool) {
}
func (d *dummyProcessorPool) execute(pod msgPod) {
	//if d.isClosed() {
	//	return
	//}

	d.routeMu.Lock()
	defer d.routeMu.Unlock()

	if d.chanClosed {
		return
	}

	d.outRoute <- pod
}
func (d *dummyProcessorPool) error(u uint8, err error) {
}
func (d *dummyProcessorPool) done() {
	d.routeMu.Lock()
	defer d.routeMu.Unlock()

	close(d.outRoute)
	d.chanClosed = true
}

func TestProcessorPool(t *testing.T) {

	t.Run("Simple Processor Pool Test", func(t *testing.T) {

		pipelineId := uint32(1)

		// Generate Message
		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		// Create pipeline and stg
		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		tStage := stgFactory.new("First Node", TRANSFORM)
		procPool := newProcessorPool(tStage)

		// Create a route for the incoming messages for a processor
		routeParam := msgRouteParam("path1")
		route := msgRoutes{
			routeParam: struct{}{},
		}

		// Add a processor to the pool with a dummy executor that simply returns the incoming messages
		pr := procPool.add(newDummyExecutor(TRANSFORM), route)

		// Add a channel where the processor can dump it's output
		stg := stgFactory.new("Second Node", TRANSFORM)
		pr.addSendTo(stg, "test")
		receiver := pr.channelForStageId(stg)

		procPool.lock(route)

		t.Run("Test1", func(t *testing.T) {
			msgPack := msgPod{
				msg:   msg,
				route: routeParam,
			}

			procPool.execute(msgPack)
			select {
			case rcvd := <-receiver:
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg.Id(), m.Id())
			}
		})

		t.Run("Test2", func(t *testing.T) {
			msg2 := msgF.NewExecute(msg, content)
			msgPack := msgPod{
				msg:   msg2,
				route: routeParam,
			}

			procPool.execute(msgPack)
			select {
			case rcvd := <-receiver:
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg2.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg2.Id(), m.Id())
			}
		})

		t.Run("Test3", func(t *testing.T) {
			msg2 := msgF.NewExecute(msg, content)
			msgPack := msgPod{
				msg:   msg2,
				route: routeParam,
			}

			procPool.execute(msgPack)
			select {
			case rcvd := <-receiver:
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg2.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg2.Id(), m.Id())
			}
		})

		procPool.done()
	})

	t.Run("Processor Pool with Two Dummy Processors", func(t *testing.T) {
		pipelineId := uint32(1)

		// Generate Message
		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		// Create pipeline and stg
		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		tStage := stgFactory.new("First Node", TRANSFORM)
		procPool := newProcessorPool(tStage)

		// Create a route for the incoming messages for a processor
		routeParam := msgRouteParam("path1")
		route := msgRoutes{
			routeParam: struct{}{},
		}

		// Create Processors
		pr1 := newDummyProcessor(newDummyExecutor(TRANSFORM), route, nil)
		pr2 := newDummyProcessor(newDummyExecutor(TRANSFORM), route, nil)

		stg1 := &stage{}
		pr1.addSendTo(stg1, "test1")
		pr1Receiver := pr1.channelForStageId(stg1)

		stg2 := &stage{}
		pr2.addSendTo(stg2, "test2")
		pr2Receiver := pr2.channelForStageId(stg2)

		msgPack := msgPod{
			msg:   msg,
			route: routeParam,
		}

		procPool.attach(pr1, pr2)
		procPool.lock(route)

		t.Run("With Both Processors", func(t *testing.T) {

			procPool.execute(msgPack)
			time.Sleep(1 * time.Microsecond)

			t.Run("Check First", func(t *testing.T) {
				rcvd := <-pr1Receiver
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg.Id(), m.Id())
			})

			t.Run("Check Second", func(t *testing.T) {
				rcvd := <-pr2Receiver
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg.Id(), m.Id())
			})
		})

		t.Run("Removing Processors", func(t *testing.T) {

			t.Run("-First", func(t *testing.T) {

				procPool.attach(pr1, pr2)
				procPool.detach(pr1)
				procPool.execute(msgPack)
				time.Sleep(1 * time.Microsecond)

				rcvd := <-pr2Receiver
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg.Id(), m.Id())

				assert.Zero(t, len(pr1Receiver))
			})

			t.Run("-Second", func(t *testing.T) {

				procPool.attach(pr1, pr2)
				procPool.detach(pr2)
				procPool.execute(msgPack)
				time.Sleep(1 * time.Microsecond)

				rcvd := <-pr1Receiver
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg.Id(), m.Id())

				assert.Zero(t, len(pr2Receiver))
			})
		})

	})
}

func BenchmarkProcessorPool(b *testing.B) {

	b.Run("Simple Processor Pool Bench", func(b *testing.B) {
		b.ReportAllocs()

		pipelineId := uint32(1)

		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		tStage := stgFactory.new("First Node", TRANSFORM)
		procPool := newProcessorPool(tStage)

		routeParam := msgRouteParam("path1")
		route := msgRoutes{
			routeParam: struct{}{},
		}

		pr := procPool.add(newDummyExecutor(TRANSFORM), route)

		stg := stgFactory.new("Second Node", TRANSFORM)
		pr.addSendTo(stg, "test")
		//receiver := pr.channelForStageId(stg)

		procPool.lock(route)

		msgPack := msgPod{
			msg:   msg,
			route: routeParam,
		}

		for i := 0; i < b.N; i++ {
			procPool.execute(msgPack)
			procPool.done()
		}
	})
}
