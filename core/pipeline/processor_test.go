package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"testing"
)

type dummyProcessorExecutor struct {
	exec       Executor
	resSrcMsg  message.Msg
	resContent message.MsgContent
}

func newDummyProcessorExecutor(exec Executor) *dummyProcessorExecutor {
	return &dummyProcessorExecutor{exec: exec}
}
func (dp *dummyProcessorExecutor) process(msg message.Msg) bool {
	return dp.exec.Execute(msg, dp)
}
func (dp *dummyProcessorExecutor) Result(srcMsg message.Msg, content message.MsgContent) {
	dp.resSrcMsg = srcMsg
	dp.resContent = content
}
func (*dummyProcessorExecutor) Error(uint8, error) {}
func (*dummyProcessorExecutor) Done()              {}

type dummyProcessor struct {
	exec     Executor
	routes   msgRoutes
	closed   bool
	outRoute sendRoute
}

func newDummyProcessor(exec Executor, routes msgRoutes) *dummyProcessor {
	return &dummyProcessor{
		exec:   exec,
		routes: routes,
		closed: false,
	}
}
func (d *dummyProcessor) Result(msg message.Msg, content message.MsgContent) {
	msgPack := msgPod{
		msg:   msg,
		route: d.outRoute.route,
	}

	d.outRoute.sendChannel <- msgPack
}
func (d *dummyProcessor) Error(uint8, error) {
	panic("implement me")
}
func (d *dummyProcessor) Done() {
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
func (d *dummyProcessor) AddSendRoute(s *stage, route sendRoute) {
	d.outRoute = route
}
func (d *dummyProcessor) isClosed() bool {

	if d.exec.ExecutorType() == SINK {
		return false
	}
	return d.closed
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

	receiver := make(chan msgPod, 1000)
	go func() {
		for pod := range receiver {
			pod.msg.Id()
		}
	}()

	proc.AddSendRoute(&stage{}, newSendRoute(receiver, "test"))
	proc.lock(nil)
	proc.sndPool.close()

	for i := 0; i < b.N; i++ {
		proc.process(proc.mesFactory.NewExecuteRoot(nil, false))
	}

	close(errRecv)
}
