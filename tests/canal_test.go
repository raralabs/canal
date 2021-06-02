package tests

import (
	"context"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/aggregates"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"github.com/raralabs/canal/utils/cast"
	"github.com/raralabs/canal/utils/timer"
	"sync/atomic"
	"testing"
	"time"
)

func TestCanal(t *testing.T) {
	// Single Stage Canal Pipeline

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(1000))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(1*time.Millisecond), "path1")

	count := aggregates.NewCount("SimpleCount", func(m map[string]interface{}) bool {
		if v, ok := m["value"]; ok {
			if vi, ok := cast.TryInt(v); ok {
				if vi%2 == 0 {
					return true
				}
			}
		}
		return false
	})

	aggs := []agg.IAggFuncTemplate{count}
	aggregator := agg.NewAggregator(aggs, nil)

	counter := p.AddTransform("Adder")
	ad := counter.AddProcessor(pipeline.DefaultProcessorOptions, aggregator.Function(), "path")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	show := int32(0)
	tmr := timer.NewTimer(100*time.Millisecond, func() {
		atomic.CompareAndSwapInt32(&show, 0, 1)
	})
	batch := p.AddTransform("Batch")
	bt := batch.AddProcessor(pipeline.DefaultProcessorOptions, doFn.BatchAgg(func(m message.Msg) bool {
		return m.Eof()
	}, func(m message.Msg) bool {
		return atomic.CompareAndSwapInt32(&show, 1, 0)
	}), "batch-path")

	delay.ReceiveFrom("path1", sp)
	counter.ReceiveFrom("path", del)
	batch.ReceiveFrom("batch-path", ad)
	sink.ReceiveFrom("sink", bt)

	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	p.Validate()

	tmr.StartTimer()
	p.Start(c, cancel)

}
