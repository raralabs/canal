package pipeline

import (
	"testing"
)

func TestProcessorPool(t *testing.T) {

}

func BenchmarkProcessorPool(b *testing.B) {
	b.ReportAllocs()

	pipelineId := uint32(1)

	pipeline := NewPipeline(pipelineId)
	stgFactory := newStageFactory(pipeline)

	tStage := stgFactory.new("First Node", TRANSFORM)
	procPool := newProcessorPool(tStage)

	pr := procPool.add(newDummyExecutor(TRANSFORM), nil)

	receiver := make(chan msgPod, 1000)
	pr.sndPool.sndRoutes[&stage{}] = newSendRoute(receiver, "test")

	procPool.lock(nil)
	procPool.close()

	for i := 0; i < b.N; i++ {
		procPool.execute(msgPod{})
	}
}
