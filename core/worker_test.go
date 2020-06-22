package core

// import (
// 	"github.com/raralabs/canal/core/message"
// 	"github.com/raralabs/canal/core/pipeline"
// 	"reflect"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// )

// func TestWorker(t *testing.T) {

// 	workerId := uint(1)

// 	var worker *Worker
// 	var n1, n2 *pipeline.pipeline

// 	t.Run("Testing Worker Creation", func(t *testing.T) {
// 		worker = NewWorker(workerId)

// 		assert.NotNil(t, worker, "Worker should have been created")
// 		assert.Equal(t, workerId, worker.Id, "id should be equal")
// 		assert.Zero(t, len(worker.host.Pipelines), "No networks have been added")
// 	})

// 	t.Run("Testing AddPipeline", func(t *testing.T) {
// 		network1Id := uint(1)
// 		network1 := worker.AddPipeline(network1Id)

// 		assert.Equal(t, 1, len(worker.host.Pipelines), "One network have been created")

// 		if n := worker.host.Pipelines[network1Id]; !reflect.DeepEqual(network1, n) {
// 			t.Errorf("pipeline: got = %#v, want = %#v", n, network1)
// 		}

// 		network2Id := uint(2)
// 		network2 := worker.AddPipeline(network2Id)

// 		assert.Equal(t, 2, len(worker.host.Pipelines), "Two networks have been created")

// 		if n := worker.host.Pipelines[network2Id]; !reflect.DeepEqual(network2, n) {
// 			t.Errorf("pipeline: got = %#v, want = %#v", n, network2)
// 		}

// 		n1 = network1
// 		n2 = network2
// 	})

// 	n1NumMsgs := int64(5)
// 	n2NumMsgs := int64(50)

// 	var n1Msg, n2Msg *message.Msg

// 	var n1Sink, n2Sink pipeline.Executor

// 	t.Run("Testing pipeline Filler", func(t *testing.T) {

// 		mf := message.NewFactory(n1.GetId(), uint32(2), uint32(1))
// 		n1Msg = mf.NewExecute(nil, &message.MsgContent{"Greet": message.NewFieldValue("Nihao", message.STRING)})

// 		n1Sink = fillDummyNetwork(n1, n1NumMsgs)
// 		// startVerifyNetwork(t, n1, n1NumMsgs, n1Msg, n1Sink)

// 		mf = message.NewFactory(n2.GetId(), uint32(2), uint32(1))
// 		n2Msg = mf.NewExecute(nil, &message.MsgContent{"Greet": message.NewFieldValue("Nihao", message.STRING)})

// 		n2Sink = fillDummyNetwork(n2, n2NumMsgs)
// 		// startVerifyNetwork(t, n2, n2NumMsgs, n2Msg, n2Sink)
// 	})
// 	// Pipelines have been filled

// 	t.Run("Testing loop", func(t *testing.T) {
// 		worker.Start(false)

// 		checkSink(t, n1NumMsgs, n1Msg, n1Sink)
// 		checkSink(t, n2NumMsgs, n2Msg, n2Sink)
// 	})

// 	_ = n1
// 	_ = n2
// }
