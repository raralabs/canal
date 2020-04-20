package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"testing"
)

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
	proc.process(msgPod{})
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
		stageId:    1,
		executor:   newDummyExecutor(TRANSFORM),
		mesFactory: message.NewFactory(1, 1, 1),
		sendPool:   newSendPool(1, 1, 1, errRecv),
	}

	receiver := make(chan msgPod, 1000)
	go func() {
		for pod := range receiver {
			pod.msg.Id()
		}
	}()

	proc.sendPool.sendRoutes[1] = newSendRoute(receiver, "test")
	proc.lock(nil)
	//proc.sendPool.close()

	for i := 0; i < b.N; i++ {
		proc.process(msgPod{})
	}
}
