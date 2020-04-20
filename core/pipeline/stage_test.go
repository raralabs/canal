package pipeline

//
//import (
//	"context"
//	"github.com/raralabs/canal/core"
//	"github.com/raralabs/canal/core/message"
//	"reflect"
//	"testing"
//
//	"github.com/stretchr/testify/assert"
//)
//
//func TestHub(t *testing.T) {
//
//	// Setup
//	pipelineId := uint(1)
//	stageId := uint32(1)
//
//	pipeline := NewPipeline(pipelineId)
//	stage := pipeline.AddTransform()
//
//	var stageProcessors []*Processor
//
//	t.Run("Testing add", func(t *testing.T) {
//		exec1 := core.newDummyExecutor()
//		exec2 := core.newDummyExecutor()
//		exec3 := core.newDummyExecutor()
//
//		stageProcessors = []*Processor{stage.AddProcessor(exec1), stage.AddProcessor(exec2), stage.AddProcessor(exec3)}
//
//		f := func() {
//			exec := core.newDummySource(1)
//			stage.AddProcessor(exec)
//		}
//		assert.Panics(t, f, "Should have panicked")
//
//		for i, j := range stageProcessors {
//			assert.Equal(t, uint(i+1), j.id, "Jobs id should match")
//			assert.Equal(t, pipelineId, j.stage.pipeline.id, "Pipeline id should be same")
//			assert.Equal(t, stageId, j.stage.id, "stage id should be same")
//		}
//	})
//
//	dummyHub := pipeline.AddTransform()
//	execs := []Executor{core.newDummyExecutor()}
//
//	var jobs []*Processor
//	for _, e := range execs {
//		jobs = append(jobs, dummyHub.AddProcessor(e))
//	}
//
//	t.Run("Testing sendRoutes", func(t *testing.T) {
//		h := stage.ReceiveFrom("default", jobs...)
//
//		// addSendTo jobs of stage to dummyHub. This just creates a cycle
//		dummyHub.ReceiveFrom("default", stageProcessors...)
//
//		assert.Equal(t, h.Id(), stage.Id(), "stage id should match")
//
//		for i, job := range h.receivePool.receiveFrom {
//			if !reflect.DeepEqual(job, jobs[i]) {
//				t.Errorf("Msg: got = %#v, want = %#v", job, jobs[i])
//			}
//
//			assert.True(t, job.isConnected(), "Job should have been connected to stage")
//		}
//
//		stage = h
//	})
//
//	t.Run("Testing initStage", func(t *testing.T) {
//		stage.lock()
//		assert.Equal(t, 1, len(stage.receivePool.receiveFrom), "1 job have been provided to stage")
//	})
//
//	t.Run("Testing loop", func(t *testing.T) {
//		callback := func() {
//
//		}
//
//		mf := message.NewFactory(pipelineId, stageId, uint32(0))
//		msg := mf.NewExecute(nil, &message.MsgContent{"Greet": message.NewFieldValue("Nihao", message.STRING)})
//		doneMsg := message.NewDoneMessage()
//
//		dummyHub.processorPool.execute(msg)
//		dummyHub.processorPool.execute(doneMsg)
//
//		stage.loop(context.Background(), callback)
//
//		//for _, job := range stage.processorPool.processors {
//		//
//		//	m := <-job.Sender
//		//	if !reflect.DeepEqual(m, msg) {
//		//		t.Errorf("Msg: got = %#v, want = %#v", m, msg)
//		//	}
//		//
//		//	m = <-job.Sender
//		//	if !reflect.DeepEqual(m, doneMsg) {
//		//		t.Errorf("Msg: got = %#v, want = %#v", m, doneMsg)
//		//	}
//		//}
//	})
//}
