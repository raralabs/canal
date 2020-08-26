package single_stage

import (
	"context"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func benchSingleStage(b *testing.B, n int64, executor pipeline.Executor) {

	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)

	p := pipeline.NewPipeline(1)
	opts := pipeline.DefaultProcessorOptions

	src := p.AddSource("Fake Source")
	sp := src.AddProcessor(opts, sources.NewFaker(n, nil))

	prc := p.AddTransform("Stage A")
	stg := prc.AddProcessor(opts, executor, "path1")

	sink := p.AddSink("Blackhole Sink")
	sink.AddProcessor(opts, sinks.NewBlackholeSink(), "sink")

	prc.ReceiveFrom("path1", sp)
	sink.ReceiveFrom("sink", stg)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()

	// Reset Timer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Start(c, cancel)
	}

	cancel()
}

func BenchmarkFilter_1M(b *testing.B) {

	flt := doFn.FilterFunction(func(m map[string]interface{}) (bool, error) {
		return true, nil
	}, func(msg message.Msg) bool {
		return false
	})

	benchSingleStage(b, 1000000, flt)
}
