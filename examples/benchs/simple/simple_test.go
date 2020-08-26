package simple

import (
	"context"
	"io/ioutil"
	"log"
	"testing"
	"time"

	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
)

func benchSimple(b *testing.B, n int64) {

	log.SetFlags(0)
	log.SetOutput(ioutil.Discard)

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Fake Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewFaker(n, nil))

	sink := p.AddSink("Blackhole Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewBlackholeSink(), "sink")

	sink.ReceiveFrom("sink", sp)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()

	// Reset Timer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Start(c, cancel)
	}

	cancel()
}

func BenchmarkSimple_1M(b *testing.B) { benchSimple(b, 1000000) }
