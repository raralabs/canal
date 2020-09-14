package pipeline

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/raralabs/canal/core/message"
)

type dummyProcessorExecutor struct {
	exec       Executor
	resSrcMsg  message.Msg
	resContent content.IContent
}

func newDummyProcessorExecutor(exec Executor) *dummyProcessorExecutor {
	return &dummyProcessorExecutor{exec: exec}
}
func (dp *dummyProcessorExecutor) process(msg message.Msg) bool {
	return dp.exec.Execute(msg, dp)
}
func (dp *dummyProcessorExecutor) Result(srcMsg message.Msg, content, prevContent content.IContent) {
	dp.resSrcMsg = srcMsg
	dp.resContent = content
}
func (dp *dummyProcessorExecutor) Persistor() IPersistor {
	return nil
}
func (*dummyProcessorExecutor) Error(uint8, error) {}
func (*dummyProcessorExecutor) Done()              {}
func (*dummyProcessorExecutor) IsClosed() bool {
	return false
}

type dummyProcessor struct {
	exec     Executor
	routes   msgRoutes
	closed   bool
	outRoute sendRoute
	prPool   IProcessorPool
	meta     *metadata
}

func newDummyProcessor(exec Executor, routes msgRoutes, prPool IProcessorPool) *dummyProcessor {
	return &dummyProcessor{
		exec:   exec,
		routes: routes,
		closed: false,
		prPool: prPool,
		meta:   newMetadata(),
	}
}
func (d *dummyProcessor) Result(msg message.Msg, content, prevContent content.IContent) {
	msgPack := msgPod{
		msg:   msg,
		route: d.outRoute.route,
	}

	d.outRoute.sendChannel <- msgPack
}
func (d *dummyProcessor) Persistor() IPersistor {
	return nil
}
func (d *dummyProcessor) Error(uint8, error) {
}
func (d *dummyProcessor) Done() {
	close(d.outRoute.sendChannel)
	d.closed = true
}
func (d *dummyProcessor) process(msg message.Msg) bool {
	return d.exec.Execute(msg, d)
}
func (d *dummyProcessor) incomingRoutes() msgRoutes {
	return d.routes
}
func (d *dummyProcessor) lock(msgRoutes) {
	return
}
func (d *dummyProcessor) IsClosed() bool {

	if d.exec.ExecutorType() == SINK {
		return false
	}
	return d.closed
}
func (d *dummyProcessor) addSendTo(s *stage, route MsgRouteParam) {
	sendChannel := make(chan msgPod, _SendBufferLength)
	d.outRoute = newSendRoute(sendChannel, route)
}
func (d *dummyProcessor) channelForStageId(stage *stage) <-chan msgPod {
	return d.outRoute.sendChannel
}
func (d *dummyProcessor) isConnected() bool {
	if d.exec.ExecutorType() == SINK {
		return true
	}
	return d.outRoute.sendChannel != nil
}
func (d *dummyProcessor) processorPool() IProcessorPool {
	return d.prPool
}
func (d *dummyProcessor) metadata() *metadata {
	return d.meta
}
//Tests
// - isConnected()
// - lock()
// - addSendTO()

func TestProcessor_Attr(t *testing.T) {
	stgFactory := newStageFactory(NewPipeline(uint32(1)))
	stg := stgFactory.new("testStg", TRANSFORM)
	stg2 := stgFactory.new("testReceivingStage",TRANSFORM)
	stg3 := stgFactory.new("testReceivingStage2",SINK)
	prcPool := newProcessorPool(stg)
	prcFactory := newProcessorFactory(stg)
	route := msgRoutes{"path1": struct{}{},"path2": struct{}{}}
	route2 := msgRoutes{"path3": struct{}{},"path4": struct{}{}}
	processor1 := prcFactory.new(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route)
	prcPool.attach(processor1)
	assert.Equal(t,false,processor1.isConnected(),"processor not connected yet.")
	assert.Panics(t, func() {
		processor1.lock(route)
	},"processor locked with out connecting")
	processor1.addSendTo(stg2,"path3")
	processor1.addSendTo(stg3,"path4")
	assert.Equal(t,true,processor1.isConnected(),"processor not connected even after making connection.")
	assert.Panics(t, func() {
		processor1.lock(route2) //subscribing the non-existing route
	},"subscribing to non existing route didn't raise error")
	processor1.lock(route)
	assert.Equal(t,true,processor1.sndPool.runLock.Load(),"sndpool lock should be enabled by lock()")
	assert.Equal(t,time.Now(),processor1.meta.startTime,"lock should initialize metadata of the processor")
}


//Test
//	- process()
func TestProcessor_process(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)

	stg1 := stgFactory.new("srcStage",SOURCE)
	stg2 := stgFactory.new("tranStage",TRANSFORM)

	prcFactoryForstg1 := newProcessorFactory(stg1)
	prcFactoryForstg2 := newProcessorFactory(stg2)

	route1 := msgRoutes{"path1": struct{}{}}
	route2 := msgRoutes{"path2": struct{}{}}

	prcForStg1 := prcFactoryForstg1.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route1)
	prcForStg2 := prcFactoryForstg2.new(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route2)
	msgFactory1 := message.NewFactory(newPipeLine.id,stg1.id,prcForStg1.id)
	msgFactory2 := message.NewFactory(newPipeLine.id,stg2.id,prcForStg2.id)
	msgContent1 := content.New()
	msgContent1.Add("greet",content.NewFieldValue("hello",content.STRING))

	msgContent2 := content.New()
	msgContent2.Add("greet",content.NewFieldValue("hello",content.STRING))
	msg1:= msgFactory1.NewExecuteRoot(msgContent1,false)
	msg2 := msgFactory2.NewExecuteRoot(msgContent2,false)
	result1 := prcForStg1.process(msg1)

	assert.Equal(t,true,result1)
	result2 := prcForStg2.process(msg2)
	assert.Equal(t,true,result2)


}




func ExpectPanic(t *testing.T) {
	if r := recover(); r == nil {
		t.Errorf("The code did not panic")
	}
}

func TestTransformFactory(t *testing.T) {
	proc := Processor{
		executor:   newDummyExecutor(SINK),
		mesFactory: message.NewFactory(1, 1, 1),
		meta:       newMetadata(),
	}

	proc.lock(nil)
	proc.process(proc.mesFactory.NewExecuteRoot(nil, false))
}

func BenchmarkProcessor(b *testing.B) {
	b.ReportAllocs()

	errRecv := make(chan message.Msg)
	go func() {
		for msg := range errRecv {
			msg.Id()
		}
	}()

	proc := Processor{
		executor:   newDummyExecutor(TRANSFORM),
		mesFactory: message.NewFactory(1, 1, 1),
		errSender:  errRecv,
	}
	proc.sndPool = newSendPool(&proc)

	stg := &stage{}
	proc.addSendTo(stg, "test")
	//receiver := pr.channelForStageId(stg)

	proc.lock(nil)
	proc.sndPool.close()

	for i := 0; i < b.N; i++ {
		proc.process(proc.mesFactory.NewExecuteRoot(nil, false))
	}

	close(errRecv)
}
