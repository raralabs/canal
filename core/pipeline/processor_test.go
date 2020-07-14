package pipeline

import (
	"testing"

	"github.com/raralabs/canal/core/message"
)

type dummyProcessorExecutor struct {
	exec       Executor
	resSrcMsg  message.Msg
	resContent *message.OrderedContent
}

func newDummyProcessorExecutor(exec Executor) *dummyProcessorExecutor {
	return &dummyProcessorExecutor{exec: exec}
}
func (dp *dummyProcessorExecutor) process(msg message.Msg) bool {
	return dp.exec.Execute(msg, dp)
}
func (dp *dummyProcessorExecutor) Result(srcMsg message.Msg, content, prevContent *message.OrderedContent) {
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
func (d *dummyProcessor) Result(msg message.Msg, content, prevContent *message.OrderedContent) {
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
