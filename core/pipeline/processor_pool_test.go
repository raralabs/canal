package pipeline

import (
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync"
	"sync/atomic"
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
	runLock    atomic.Value
}


func newDummyProcessorPool(route MsgRouteParam, stg *stage) *dummyProcessorPool {
	sendChannel := make(chan msgPod, _SendBufferLength)
	return &dummyProcessorPool{

		outRoute:   sendChannel,
		chanClosed: false,
		routeMu:    &sync.Mutex{},
		stg:        stg,
	}
}
func (d *dummyProcessorPool) add(pr ProcessorOptions, exec Executor, routes msgRoutes) IProcessor {
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
	//if d.IsClosed() {
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
//Tests
// -newProcessorPool
// - stage()
// - shortCircuitProcessors()

func Test_newProcessorPool(t *testing.T){
	stg :=  &stage{
					id: uint32(5),
		            executorType: SOURCE,
		            name:"firstStage",
	}
	t.Run("newProcessorPool", func(t *testing.T) {
		procPool:=newProcessorPool(stg)
		if !reflect.DeepEqual(stg, procPool.stg){
			t.Errorf("Want: %v\nGot: %v\n", stg, procPool.stg)}
		assert.Equal(t,false,procPool.shortCircuit,"short circuit must be disabled by default")
		assert.Equal(t,"map[pipeline.MsgRouteParam][]pipeline.IProcessorForPool",reflect.TypeOf(procPool.procMsgPaths).String(),"Datatype of the created procMsgPaths incompatible")
		assert.Equal(t,"pipeline.processorFactory",reflect.TypeOf(procPool.processorFactory).String(),"creating new proc pool should create processor Factory")
		assert.Equal(t,stg.id,procPool.processorFactory.stage.id,"processor factory not in same stage")
		assert.Equal(t, nil,procPool.runLock.Load(),"processor should not be runLock at the initialization")
		assert.Equal(t,procPool.stg,procPool.stage(),"Unexpected value received for stage")
	})
}

func TestProcessorPool_add(t *testing.T){
	newPipeline:= NewPipeline(uint32(1))
	stgFac := newStageFactory(newPipeline)
	stg:= stgFac.new("Stagedummy",TRANSFORM)
	procPool := newProcessorPool(stg)
	route := msgRoutes{	"path1": struct{}{},}
	stg.processorPool = procPool
	assert.Equal(t,0,len(procPool.procMsgPaths),"inital condition there is shouldn't be any procesor")//no processor added
	procPool.add(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route)
	assert.Equal(t,1,len(procPool.procMsgPaths),"number of processor added didn't match the route count for procesor")
}

func TestProcessorPool_shortCircuit(t *testing.T){
	stg :=  &stage{
		id: uint32(5),
		executorType: SOURCE,
		name:"firstStage",
	}
	procPool := newProcessorPool(stg)
	t.Run("shortCircuitProcessor", func(t *testing.T) {
		assert.Equal(t,false,procPool.shortCircuit,"shortcircuit must be disabled by default")
		procPool.shortCircuitProcessors()
		assert.Equal(t,true,procPool.shortCircuit,"shortcircuit enabled but no change detected")

	})
}

//Test:
// -attach()
// -detach() processors
// -removeProcRecv
func TestProcessorPool_Operations(t *testing.T){
	pipe := NewPipeline(uint32(1))
	stgFactory := newStageFactory(pipe)
	stg:= stgFactory.new("Stage1",SOURCE)
	procPool := newProcessorPool(stg)
	route := msgRoutes{"path": struct{}{},}
	otherRoute := msgRoutes{"path2": struct{}{}}
	processor1:= newDummyProcessor(newDummyExecutor(SOURCE),route,procPool)
	processor2:= newDummyProcessor(newDummyExecutor(SOURCE),route,procPool)
	//test for attach processor
	//subscribing to same route
	assert.Equal(t,0,len(procPool.procMsgPaths),"no process attached yet")
	procPool.attach(processor1)
	assert.Equal(t,1,len(procPool.procMsgPaths["path"]),"process attached")
	procPool.attach(processor2)
	assert.Equal(t,2,len(procPool.procMsgPaths["path"]),"second process attached")
	//subscribing to other route
	processor3 := newDummyProcessor(newDummyExecutor(SOURCE),otherRoute,procPool)
	procPool.attach(processor3)
	assert.Equal(t,2,len(procPool.procMsgPaths),"there are two different path in total")
	assert.Equal(t,1,len(procPool.procMsgPaths["path2"]),"only one processor subscribed by processor3")

	//test for detach processor
	procPool.detach(processor3)
	assert.Equal(t,0,len(procPool.procMsgPaths["path2"]),"one processor subscribing to other route removed")
	procPool.detach(processor2)
	assert.Equal(t,1,len(procPool.procMsgPaths["path"]),"one processor subscribing to other route removed")
	procPool.detach(processor1)
	assert.Equal(t,0,len(procPool.procMsgPaths["path"]),"one processor subscribing to other route removed")

}

//Tests:
//	- lock()
// 	- isRunning()
func TestProcessorPool_lock(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)
	stg:= stgFactory.new("stage1",SOURCE)
	procPool := newProcessorPool(stg)
	stg.processorPool = procPool
	route := msgRoutes{"path": struct{}{}}
	t.Run("lock processor pool with process", func(t *testing.T) {

		assert.Panics(t, func() {
			stg.processorPool.lock(route)//trying to lock procesorPool without processor
		})
		pr1 :=  newDummyProcessor(newDummyExecutor(TRANSFORM),route,stg.processorPool)
		stg.processorPool.attach(pr1)
		assert.Equal(t,false,procPool.isRunning(),"processor should not be running before lock method invocation")
		stg.processorPool.lock(route)
		assert.Equal(t,true,procPool.runLock.Load(),"lock must have been enabled")
		assert.Equal(t,true,procPool.isRunning(),"the processor is running but shows false")
	})

}


func TestProcessorPool(t *testing.T) {

	t.Run("Simple Processor Pool Test", func(t *testing.T) {

		pipelineId := uint32(1)

		// Generate Message
		msgF := message.NewFactory(pipelineId, 1, 1)
		content := content2.New()
		content = content.Add("value", content2.NewFieldValue(12, content2.INT))

		msg := msgF.NewExecuteRoot(content, false)

		// Create pipeline and stg
		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		tStage := stgFactory.new("First Node", TRANSFORM)
		procPool := newProcessorPool(tStage)

		// Create a route for the incoming messages for a processor
		routeParam := MsgRouteParam("path1")
		route := msgRoutes{
			routeParam: struct{}{},
		}

		// Add a processor to the pool with a dummy executor that simply returns the incoming messages
		pr := procPool.add(DefaultProcessorOptions, newDummyExecutor(TRANSFORM), route)

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
			msg2 := msgF.NewExecute(msg, content, nil)
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
			msg2 := msgF.NewExecute(msg, content, nil)
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
		content := content2.New()
		content = content.Add("value", content2.NewFieldValue(12, content2.INT))

		msg := msgF.NewExecuteRoot(content, false)

		// Create pipeline and stg
		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		tStage := stgFactory.new("First Node", TRANSFORM)
		procPool := newProcessorPool(tStage)

		// Create a route for the incoming messages for a processor
		routeParam := MsgRouteParam("path1")
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
		content := content2.New()
		content = content.Add("value", content2.NewFieldValue(12, content2.INT))

		msg := msgF.NewExecuteRoot(content, false)

		pipeline := NewPipeline(pipelineId)
		stgFactory := newStageFactory(pipeline)

		tStage := stgFactory.new("First Node", TRANSFORM)
		procPool := newProcessorPool(tStage)

		routeParam := MsgRouteParam("path1")
		route := msgRoutes{
			routeParam: struct{}{},
		}

		pr := procPool.add(DefaultProcessorOptions, newDummyExecutor(TRANSFORM), route)

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
