package main

import (
	"context"
	"time"

	"github.com/raralabs/canal/ext/transforms/aggregates/templates"

	"github.com/raralabs/canal/ext/transforms/doFn"

	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"

	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
)

func main() {

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(10))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")

	count := templates.NewCount("SimpleCount", func(m map[string]interface{}) bool {
		return true
	})

	avg := templates.NewVariance("Sample-Variance", "value", func(m map[string]interface{}) bool {
		return true
	})

	aggs := []agg.IAggFuncTemplate{avg, count}
	aggregator := agg.NewAggregator(aggs, nil)

	counter := p.AddTransform("Adder")
	ad := counter.AddProcessor(pipeline.DefaultProcessorOptions, aggregator.Function(), "path")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	delay.ReceiveFrom("path1", sp)
	counter.ReceiveFrom("path", del)
	sink.ReceiveFrom("sink", ad)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()

	p.Start(c, cancel)
}
