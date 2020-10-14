package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
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
	msgPod := MsgPod{Msg: msg,
					Route: ""}
	return dp.exec.Execute(msgPod, dp)
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
	msgPack := MsgPod{
		Msg:   msg,
		Route: d.outRoute.route,
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
func (d *dummyProcessor) process(msgPod MsgPod) bool {
	return d.exec.Execute(msgPod, d)
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
	sendChannel := make(chan MsgPod, _SendBufferLength)
	d.outRoute = newSendRoute(sendChannel, route)
}
func (d *dummyProcessor) channelForStageId(stage *stage) <-chan MsgPod {
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
// - metadata()
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
	//processor1.lock(route)
	//tie:=time.Now()
	assert.Equal(t,nil,processor1.sndPool.runLock.Load(),"sndpool lock should be enabled by lock()")
	hours, minutes,_:= time.Now().Clock()
	processor1.lock(route)
	processor1StHr,processor1StMin,_ := processor1.meta.startTime.Clock()
	assert.Equal(t,hours,processor1StHr,"lock should initialize metadata of the processor")
	assert.Equal(t,minutes,processor1StMin,"start time mismatch")
	assert.Equal(t,processor1.meta,processor1.metadata(),"meta data returned by metadata() must match")
}


//Test
//	- process()
// 	- incomingRoutes()
//  - Done()
//  - statusMessage()
func TestProcessor_process(t *testing.T){
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)

	stg1 := stgFactory.new("srcStage",SOURCE)
	stg2 := stgFactory.new("tranStage",TRANSFORM)
	prcPool1 := newProcessorPool(stg1)
	prcPool2 := newProcessorPool(stg2)


	prcFactoryForstg1 := newProcessorFactory(stg1)
	prcFactoryForstg2 := newProcessorFactory(stg2)

	route1 := msgRoutes{"path1": struct{}{}}
	route2 := msgRoutes{"path2": struct{}{}}

	prcForStg1 := prcFactoryForstg1.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route1)
	prcForStg2 := prcFactoryForstg2.new(DefaultProcessorOptions,newDummyExecutor(TRANSFORM),route2)

	prcPool1.attach(prcForStg1)
	prcPool2.attach(prcForStg2)

	stg1.processorPool = prcPool1
	stg2.processorPool = prcPool2

	msgFactory1 := message.NewFactory(newPipeLine.id,stg1.id,prcForStg1.id)
	msgFactory2 := message.NewFactory(newPipeLine.id,stg2.id,prcForStg2.id)
	msgContent1 := content.New()
	msgContent1.Add("greet",content.NewFieldValue("hello",content.STRING))

	msgContent2 := content.New()
	msgContent2.Add("greet",content.NewFieldValue("hello",content.STRING))
	msg1:= msgFactory1.NewExecuteRoot(msgContent1,false)
	msg2 := msgFactory2.NewExecuteRoot(msgContent2,false)

	result1 := prcForStg1.process(MsgPod{Msg:msg1})

	assert.Equal(t,true,result1)
	result2 := prcForStg2.process(MsgPod{Msg:msg2})
	assert.Equal(t,true,result2)
	//prcForStg1.Done()
	//prcForStg2.Done()
	//done???
	assert.Equal(t,true,prcForStg1.IsClosed(),"processor 1 already closed")
	assert.Equal(t,true,prcForStg2.IsClosed(),"processor 2 is not closed")
	//possible bug
	//assert.Equal(t,prcForStg1.processorPool(),prcPool1,"processor pool must be same")
	//assert.Equal(t,prcPool2,stg2.processorPool,"processor pool must be same")
	//test incomingRouts()
	assert.Equal(t,route1,prcForStg1.incomingRoutes(),"routes mismatch")
	assert.Equal(t,route2,prcForStg2.incomingRoutes(),"routes mismatch")
	assert.Equal(t,"message.Msg",reflect.TypeOf(prcForStg1.statusMessage(true)).String())

	currContent := content.New()
	currContent.Add("abc",content.NewFieldValue("good",content.STRING))
	t.Run("result", func(t *testing.T) {
		prcForStg1.Result(msg1,currContent,msgContent1)
	})

}

//func TestProcessor_Result(t *testing.T) {
//
//}

func TestProcessor_Persistor(t *testing.T) {
	newPipeLine := NewPipeline(uint32(1))
	stgFactory := newStageFactory(newPipeLine)
	stg1 := stgFactory.new("srcStage",SOURCE)
	prcFactoryForstg1 := newProcessorFactory(stg1)
	route1 := msgRoutes{"path1": struct{}{}}
	prc1 := prcFactoryForstg1.new(DefaultProcessorOptions,newDummyExecutor(SOURCE),route1)
	assert.Equal(t,nil,prc1.Persistor(),"persitor should be set to nil for default procesor")
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
	proc.process(MsgPod{Msg:proc.mesFactory.NewExecuteRoot(nil, false)})
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
		proc.process(MsgPod{Msg:proc.mesFactory.NewExecuteRoot(nil, false)})
	}

	close(errRecv)
}
