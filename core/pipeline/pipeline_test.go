package pipeline

//
//import (
//	"context"
//	"github.com/raralabs/canal/core/message"
//	"reflect"
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//func TestNetwork(t *testing.T) {
//
//	// Setup
//	pipelineId := uint(1)
//	srcHubId := uint32(1)
//	prcHubId := uint32(2)
//
//	numMsgs := int64(3)
//
//	var pipeline *Pipeline
//
//	var srcJob, prcJob *Processor
//
//	t.Run("Testing pipeline Creation", func(t *testing.T) {
//		pipeline = NewPipeline(pipelineId)
//
//		assert.NotNil(t, pipeline, "pipeline should have been created")
//		assert.Equal(t, pipelineId, pipeline.id, "id should be equal")
//		assert.Zero(t, len(pipeline.stages), "No hubs have been added")
//	})
//
//	var srcHub *stg
//	t.Run("Testing AddSource", func(t *testing.T) {
//		pipeline.AddSource()
//		assert.Equal(t, 1, len(pipeline.stages), "Only one hub added to pipeline")
//		srcHub = pipeline.stages[0]
//		assert.Equal(t, SOURCE, srcHub.executorType, "stg is a source")
//
//		// add source job(transforms) to the hub
//		//exec := core.newDummySource(numMsgs)
//		//srcJob = pipeline.stages[0].AddProcessor(exec)
//	})
//
//	t.Run("Testing AddTransform", func(t *testing.T) {
//		pipeline.AddTransform()
//		assert.Equal(t, 2, len(pipeline.stages), "Two hubs added to pipeline")
//		prcHub := pipeline.stages[1]
//		assert.Equal(t, TRANSFORM, prcHub.executorType, "stg is a source")
//
//		// add transforms job(transforms) to the hub
//		//exec := core.newDummyExecutor()
//		//prcJob = pipeline.stages[1].AddProcessor(exec)
//		//
//		//pipeline.stages[1].ReceiveFrom("default", srcJob)
//	})
//
//	var snkExec Executor
//
//	t.Run("Testing AddSink", func(t *testing.T) {
//		pipeline.AddSink()
//		assert.Equal(t, 3, len(pipeline.stages), "Three hubs added to pipeline")
//		snkHub := pipeline.stages[2]
//		assert.Equal(t, SINK, snkHub.executorType, "stg is a source")
//
//		// add transforms job(transforms) to the hub
//		//snkExec = core.newDummySink()
//		//pipeline.stages[2].AddProcessor(snkExec)
//		//
//		//pipeline.stages[2].ReceiveFrom("default", prcJob)
//	})
//
//	t.Run("Testing loop and Wait", func(t *testing.T) {
//
//		f := func() {
//
//			mf := message.NewFactory(pipelineId, prcHubId, prcJob.id)
//			msg := mf.NewExecute(nil, &message.MsgContent{"Greet": message.NewFieldValue("Nihao", message.STRING)})
//
//			for i := int64(0); i < numMsgs; i++ {
//				// Correct msg Ids
//				//msg.id = uint64(2 * (i + 1))
//				//if s, ok := snkExec.(*core.dummySink); ok {
//				//	m := <-s.sunk
//				//	if !reflect.DeepEqual(m, msg) {
//				//		t.Errorf("Msg: got = %#v, want = %#v", m, msg)
//				//	}
//				//}
//			}
//		}
//
//		pipeline.Start(context.Background(), f)
//	})
//
//	t.Run("Testing Dummy pipeline Filler and Checker", func(t *testing.T) {
//		//n := NewPipeline(uint(1))
//		//
//		//mf := msg.NewFactory(pipelineId, prcHubId, prcJob.id)
//		//msg := mf.NewExecute(nil, &msg.MsgContent{"Greet": msg.NewFieldValue("Nihao", msg.STRING)})
//
//		//snkExec := core.fillDummyNetwork(n, 3)
//		//core.startVerifyNetwork(t, n, numMsgs, msg, snkExec)
//	})
//
//	t.Run("Testing GetStage and RemoveLastNodes", func(t *testing.T) {
//		hub := pipeline.GetStage(srcHubId)
//		if !reflect.DeepEqual(hub, srcHub) {
//			t.Errorf("Msg: got = %#v, want = %#v", hub, srcHub)
//		}
//
//		pipeline.RemoveLastNodes(1)
//		assert.Equal(t, 2, len(pipeline.stages), "One node removed")
//	})
//}
