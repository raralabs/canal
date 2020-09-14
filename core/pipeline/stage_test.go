package pipeline

import (
	"context"
	"github.com/stretchr/testify/assert"
	"time"
	//"github.com/raralabs/canal/core/pipeline"
	"reflect"
	"testing"
)


func TestStage_GetId(t *testing.T) {
	pipeLines := []*Pipeline{NewPipeline(uint32(1)),NewPipeline(uint32(5)),NewPipeline(uint32(7)),}
	for _,pipeLine := range pipeLines{
		stg:= stage{
			id: 10,
			name :"dummy stage",
			pipeline: pipeLine,
		}
		assert.Equal(t,uint32(10),stg.GetId(),"Result: %d doesn't match expected:  %d",uint32(10),stg.GetId())
		assert.Equal(t,pipeLine,stg.pipeline,"Stage must be in the allocated pipeline")

	}
}


//Tests:
// -newStageFactory
// - AddProcessor
func TestStage_AddProcessor(t *testing.T) {
	type testCase struct{
		testName			string
		stgExecType			ExecutorType
		routes 				MsgRouteParam
	}
	testPipeline := NewPipeline(uint32(1))
	tests := []*testCase{
	{"test with SOURCE ExecutorType",SOURCE,""},
	{"test with SOURCE ExecutorType",SOURCE,""},
	{"test with Transform ExecutorType",TRANSFORM,"path1"},
	{"test with SOURCE ExecutorType",SINK,"path1"},
	}
	stgFactory := newStageFactory(testPipeline)
	for _,test := range tests{
		stg := stgFactory.new("newStage",test.stgExecType)
		assert.Equal(t,testPipeline,stg.pipeline,"stage should belong to the pipeline specified")
		assert.Equal(t,test.stgExecType,stg.executorType,"Executor didn't match")
		assert.Equal(t,"newStage",stg.name,"Name must match")
		stg.processorPool = newDummyProcessorPool(test.routes,stg)
		switch test.stgExecType {
		case SOURCE:
			stg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(SOURCE))
			continue
		case TRANSFORM:
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(TRANSFORM))
			continue
		case SINK:
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(SINK))
		}

		assert.Equal(t,test.stgExecType,stg.executorType,"executor must match")

	otherStg := stgFactory.new("otherStage",SOURCE)
	assert.Panics(t, func() {
		otherStg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM))
		})
	assert.Panics(t, func() {
			otherStg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(SINK))
		})
	bckGnd := context.Background()
	d := time.Now().Add(10 * time.Millisecond)
	ctx, _ := context.WithDeadline(bckGnd, d)
	otherStg.loop(ctx,func(){})
	assert.Panics(t,func(){
		otherStg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM)) //can't add processor to running stage
	})
	}

}

func TestStage_ReceiveFrom(t *testing.T) {
	newPipeline := NewPipeline(uint32(1))
	stgFactry := newStageFactory(newPipeline)
	sendingStage := stgFactry.new("sender",SOURCE)
	receivingStage := stgFactry.new("receiver",TRANSFORM)
	prPool := newDummyProcessorPool("path1", sendingStage)
	sendingStage.processorPool = prPool
	route := msgRoutes{
		"path2": struct{}{},
	}
	processor1 := newDummyProcessor(newDummyExecutor(SOURCE),route,sendingStage.processorPool)
	recPool := newDummyReceivePool(receivingStage)
	receivingStage.receivePool = recPool

	t.Run("ReceiveFrom", func(t *testing.T) {
		receivingStage.ReceiveFrom("", processor1)
	})

}


//Test
// - isRunning()
// - isClosed()
func TestStage_States(t *testing.T) {
	newPipeline := NewPipeline(uint32(1))
	stgFactory :=  newStageFactory(newPipeline)
	stg := stgFactory.new("stage1",TRANSFORM)
	bckGnd := context.Background()
	d := time.Now().Add(10 * time.Millisecond)
	ctx, _ := context.WithDeadline(bckGnd, d)
	assert.Equal(t,false,stg.isRunning(),"stage started before loop func is called")
	assert.Equal(t,false,stg.isClosed(),"stage closed before loop func is called")
	stg.loop(ctx,func() {
		assert.Equal(t,true,stg.isRunning(),"stage not running even after invoking loop")
		assert.Equal(t,true,stg.isClosed(),"stage not closed after loop func is called")
					})
}


func TestStage_String(t *testing.T){
	stg := &stage{id:1,
		executorType: TRANSFORM,
		}
	assert.Equal(t,"string",reflect.TypeOf(stg.String()).String(),"data type must be string")
}

func TestStage_Trace(t *testing.T) {
	type testCase struct{
		testName 	string
		stg			*stage
		wantErr		bool
	}

	tests := []testCase{
		{"Stage SOURCE TYPE with trace flag set true",
			&stage{id:1,executorType: SOURCE},false},

		{"Stage TRANSFORM TYPE with trace flag set true",
			&stage{id:1,executorType: TRANSFORM},true},

		{"Stage TRANSFORM TYPE with trace flag set false",
			&stage{id:1,executorType: SINK},true},
		}

	for _,test := range tests{
		if test.wantErr==true{
			assert.Panics(t,func(){
				test.stg.Trace()
			})
		}else{
			test.stg.Trace()
			assert.Equal(t,true,test.stg.withTrace,"Trace couldn't be enabled")
		}
	}
}

//Tests
// -lock
func Test_Lock (t *testing.T){
	stgFactory := newStageFactory(NewPipeline(uint32(1)))

	testStage1 := stgFactory.new("stage1",TRANSFORM)
	prcPool := newDummyProcessorPool("path1", testStage1)
	recPool := newDummyReceivePool(testStage1)
	route := msgRoutes{	"path1": struct{}{},
						"path2": struct{}{},
						"path3": struct{}{},
					}
	testStage1.processorPool = prcPool
	testStage1.receivePool = recPool
	testStage1.processorPool.add(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route)
	testStage1.processorPool.add(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route)
	testStage2 := stgFactory.new("Stage2",TRANSFORM)
	testStage2.processorPool = prcPool
	t.Run("source loop", func(t *testing.T) {
		testStage1.lock() // locking stage that has atleast one processor
		assert.Panics(t,func(){
			testStage2.lock() //locking stage that doesn't have processor
		})
		status :=testStage1.processorPool.add(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route) //trying to add
			//to the running stage
		assert.Equal(t,nil,status,"Error :added to the running stage")
	})


}

//func TestStage(t *testing.T) {
//
//	t.Run("Simple Tests", func(t *testing.T) {
//		pipelineId := uint32(1)
//
//		// Create pipeline and stg
//		pipeline := NewPipeline(pipelineId)
//
//		t.Run("Source", func(t *testing.T) {
//
//			stg := &stage{
//				id:           1,
//				name:         "First Node",
//				pipeline:     pipeline,
//				executorType: SOURCE,
//				errorSender:  pipeline.errorReceiver,
//				routes:       make(msgRoutes),
//				withTrace:    false,
//			}
//
//			prPool := newDummyProcessorPool("path1", stg)
//			stg.processorPool = prPool
//			stg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(SOURCE))
//
//			bckGnd := context.Background()
//			d := time.Now().Add(10 * time.Millisecond)
//			ctx, cancel := context.WithDeadline(bckGnd, d)
//
//			stg.lock()
//			go stg.loop(ctx, func() {
//
//			})
//
//			go func() {
//			rcvLoop:
//				for {
//					rcvd, ok := <-prPool.outRoute
//					if !ok {
//						break rcvLoop
//					}
//					m := msgPod{}
//					if rcvd.route != m.route {
//						t.Errorf("Want: %v\nGot: %v\n", m.route, rcvd.route)
//					}
//				}
//			}()
//
//			time.Sleep(1 * time.Millisecond)
//
//			assert.Panics(t, func() {
//				stg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(SOURCE))
//			})
//
//			prPool.done()
//			cancel()
//
//			assert.Panics(t, func() {
//				stg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM))
//			})
//			assert.Panics(t, func() {
//				stg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(SOURCE), "path1", "path2")
//			})
//		})
//
//		t.Run("TRANSFORM", func(t *testing.T) {
//
//			stg := &stage{
//				id:           1,
//				name:         "First Node",
//				pipeline:     pipeline,
//				executorType: TRANSFORM,
//				errorSender:  pipeline.errorReceiver,
//				routes:       make(msgRoutes),
//				withTrace:    false,
//			}
//
//			prPool := newDummyProcessorPool("path1", stg)
//			rcvPool := newDummyReceivePool(stg)
//
//			stg.processorPool = prPool
//			stg.receivePool = rcvPool
//			stg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM), "path1")
//
//			sendStg := &stage{
//				id:           2,
//				name:         "Genesis Node",
//				pipeline:     pipeline,
//				executorType: TRANSFORM,
//				errorSender:  pipeline.errorReceiver,
//				routes:       make(msgRoutes),
//				withTrace:    false,
//			}
//			sendPrPool := newDummyProcessorPool("path2", sendStg)
//			sendStg.processorPool = sendPrPool
//			sendStg.receivePool = newDummyReceivePool(sendStg)
//			route := msgRoutes{
//				"path2": struct{}{},
//			}
//			pr1 := newDummyProcessor(newDummyExecutor(TRANSFORM), route, sendPrPool)
//			pr1.addSendTo(stg, "path")
//
//			stg.ReceiveFrom("path", pr1)
//
//			ctx := context.Background()
//			stg.lock()
//			sendStg.lock()
//			go stg.loop(ctx, func() {
//			})
//
//			msgF := message.NewFactory(pipelineId, 3, 1)
//			content := content2.New()
//			content = content.Add("value", content2.NewFieldValue(12, content2.INT))
//
//			msg := msgF.NewExecuteRoot(content, false)
//
//			pr1.process(msg)
//
//			go func() {
//			rcvLoop:
//				for {
//					rcvd, ok := <-prPool.outRoute
//					if !ok {
//						break rcvLoop
//					}
//					m := msg
//					if !reflect.DeepEqual(rcvd.msg.Content(), m.Content()) {
//						t.Errorf("Want: %v\nGot: %v\n", m.Content(), rcvd.msg.Content())
//					}
//				}
//			}()
//
//			time.Sleep(1 * time.Millisecond)
//
//			prPool.done()
//
//		})
//
//	})
//
//}
