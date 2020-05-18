package pipeline

import (
	"context"
	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

func TestStage(t *testing.T) {

	t.Run("Simple Tests", func(t *testing.T) {
		pipelineId := uint32(1)

		// Create pipeline and stg
		pipeline := NewPipeline(pipelineId)

		t.Run("Source", func(t *testing.T) {

			stg := &stage{
				id:           1,
				name:         "First Node",
				pipeline:     pipeline,
				executorType: SOURCE,
				errorSender:  pipeline.errorReceiver,
				routes:       make(msgRoutes),
				withTrace:    false,
			}

			prPool := newDummyProcessorPool("path1", stg)
			stg.processorPool = prPool
			stg.AddProcessor(newDummyExecutor(SOURCE))

			bckGnd := context.Background()
			d := time.Now().Add(10 * time.Millisecond)
			ctx, cancel := context.WithDeadline(bckGnd, d)

			stg.lock()
			go stg.loop(ctx, func() {

			})

			go func() {
			rcvLoop:
				for {
					rcvd, ok := <-prPool.outRoute
					if !ok {
						break rcvLoop
					}
					m := msgPod{}
					if rcvd.route != m.route {
						t.Errorf("Want: %v\nGot: %v\n", m.route, rcvd.route)
					}
				}
			}()

			time.Sleep(1 * time.Millisecond)

			assert.Panics(t, func() {
				stg.AddProcessor(newDummyExecutor(SOURCE))
			})

			prPool.done()
			cancel()

			assert.Panics(t, func() {
				stg.AddProcessor(newDummyExecutor(TRANSFORM))
			})
			assert.Panics(t, func() {
				stg.AddProcessor(newDummyExecutor(SOURCE), "path1", "path2")
			})
		})

		t.Run("TRANSFORM", func(t *testing.T) {

			stg := &stage{
				id:           1,
				name:         "First Node",
				pipeline:     pipeline,
				executorType: TRANSFORM,
				errorSender:  pipeline.errorReceiver,
				routes:       make(msgRoutes),
				withTrace:    false,
			}

			prPool := newDummyProcessorPool("path1", stg)
			rcvPool := newDummyReceivePool(stg)

			stg.processorPool = prPool
			stg.receivePool = rcvPool
			stg.AddProcessor(newDummyExecutor(TRANSFORM), "path1")

			sendStg := &stage{
				id:           2,
				name:         "Genesis Node",
				pipeline:     pipeline,
				executorType: TRANSFORM,
				errorSender:  pipeline.errorReceiver,
				routes:       make(msgRoutes),
				withTrace:    false,
			}
			sendPrPool := newDummyProcessorPool("path2", sendStg)
			sendStg.processorPool = sendPrPool
			sendStg.receivePool = newDummyReceivePool(sendStg)
			route := msgRoutes{
				"path2": struct{}{},
			}
			pr1 := newDummyProcessor(newDummyExecutor(TRANSFORM), route, sendPrPool)
			pr1.addSendTo(stg, "path")

			stg.ReceiveFrom("path", pr1)

			ctx := context.Background()
			stg.lock()
			sendStg.lock()
			go stg.loop(ctx, func() {
			})

			msgF := message.NewFactory(pipelineId, 3, 1)
			content := message.MsgContent{
				"value": message.NewFieldValue(12, message.INT),
			}
			msg := msgF.NewExecuteRoot(content, false)

			pr1.process(msg)

			go func() {
			rcvLoop:
				for {
					rcvd, ok := <-prPool.outRoute
					if !ok {
						break rcvLoop
					}
					m := msg
					if !reflect.DeepEqual(rcvd.msg.Content(), m.Content()) {
						t.Errorf("Want: %v\nGot: %v\n", m.Content(), rcvd.msg.Content())
					}
				}
			}()

			time.Sleep(1 * time.Millisecond)

			prPool.done()

		})

	})

}

func BenchmarkStage(b *testing.B) {

	b.Run("Simple Benchs", func(b *testing.B) {
		pipelineId := uint32(1)

		// Create pipeline and stg
		pipeline := NewPipeline(pipelineId)

		b.Run("Sources", func(b *testing.B) {

			stg := &stage{
				id:           1,
				name:         "First Node",
				pipeline:     pipeline,
				executorType: SOURCE,
				errorSender:  pipeline.errorReceiver,
				routes:       make(msgRoutes),
				withTrace:    false,
			}

			prPool := newDummyProcessorPool("path1", stg)
			stg.processorPool = prPool
			stg.AddProcessor(newDummyExecutor(SOURCE))

			bckGnd := context.Background()
			d := time.Now().Add(10 * time.Millisecond)
			ctx, cancel := context.WithDeadline(bckGnd, d)

			stg.lock()

			for i := 0; i < b.N; i++ {

				go stg.loop(ctx, func() {
				})

				go func() {
				rcvLoop:
					for {
						_, ok := <-prPool.outRoute
						if !ok {
							break rcvLoop
						}
					}
				}()
			}

			prPool.done()
			cancel()
		})

	})

}
