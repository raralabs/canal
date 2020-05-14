package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestProcessorPool(t *testing.T) {

	t.Run("Simple Processor Pool Test", func(t *testing.T) {

		pipelineId := uint32(1)

		// Generate Message
		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		// Create pipeline and stage
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
		receiver := make(chan msgPod, 1000)
		pr.AddSendRoute(&stage{}, newSendRoute(receiver, "test"))

		procPool.lock(route)

		t.Run("Test1", func(t *testing.T) {
			msgPack := msgPod{
				msg:   msg,
				route: routeParam,
			}

			procPool.Execute(msgPack)
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

			procPool.Execute(msgPack)
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

			procPool.Execute(msgPack)
			select {
			case rcvd := <-receiver:
				m := rcvd.msg
				if !reflect.DeepEqual(m.Content(), msg2.Content()) {
					t.Errorf("Want: %v\nGot: %v\n", msg.Content(), m.Content())
				}
				assert.Equal(t, msg2.Id(), m.Id())
			}
		})

		procPool.Done()
	})

	t.Run("Processor Pool with Two Dummy Processors", func(t *testing.T) {
		pipelineId := uint32(1)

		// Generate Message
		msgF := message.NewFactory(pipelineId, 1, 1)
		content := message.MsgContent{
			"value": message.NewFieldValue(12, message.INT),
		}
		msg := msgF.NewExecuteRoot(content, false)

		// Create pipeline and stage
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
		pr1 := newDummyProcessor(newDummyExecutor(TRANSFORM), route)
		pr2 := newDummyProcessor(newDummyExecutor(TRANSFORM), route)

		pr1Receiver := make(chan msgPod, 100)
		pr2Receiver := make(chan msgPod, 100)

		pr1.AddSendRoute(&stage{}, newSendRoute(pr1Receiver, "test1"))
		pr2.AddSendRoute(&stage{}, newSendRoute(pr2Receiver, "test2"))

		msgPack := msgPod{
			msg:   msg,
			route: routeParam,
		}

		procPool.Attach(pr1, pr2)
		procPool.lock(route)

		t.Run("With Both Processors", func(t *testing.T) {

			procPool.Execute(msgPack)
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

				procPool.Attach(pr1, pr2)
				procPool.Detach(pr1)
				procPool.Execute(msgPack)
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

				procPool.Attach(pr1, pr2)
				procPool.Detach(pr2)
				procPool.Execute(msgPack)
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

		close(pr1Receiver)
		close(pr2Receiver)
	})
}

func BenchmarkProcessorPool(b *testing.B) {
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

	receiver := make(chan msgPod, 1000)
	pr.AddSendRoute(&stage{}, newSendRoute(receiver, "test"))

	procPool.lock(route)

	msgPack := msgPod{
		msg:   msg,
		route: routeParam,
	}

	for i := 0; i < b.N; i++ {
		procPool.Execute(msgPack)
		procPool.Done()
	}
}
