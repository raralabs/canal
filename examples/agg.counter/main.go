package main

import (
	"context"
	"time"

	"github.com/raralabs/canal/ext/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/event/poll"

	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms"
)

func main() {

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(10))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(pipeline.DefaultProcessorOptions, transforms.DelayFunction(100*time.Millisecond), "path1")

	count := agg.NewCount("SimpleCount", func(m map[string]interface{}) bool {
		return true
	})

	mean := agg.NewMean("SimpleMean", "value", func(m map[string]interface{}) bool {
		return true
	})

	aggs := []agg.Aggregator{count, mean}
	filter := poll.NewFilterEvent(func(map[string]interface{}) bool {
		return true
	})
	aggregator := transforms.NewAggregator(filter, aggs, nil)

	counter := p.AddTransform("Adder")
	ad := counter.AddProcessor(pipeline.DefaultProcessorOptions, aggregator.Function(), "path")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	delay.ReceiveFrom("path1", sp)
	counter.ReceiveFrom("path", del)
	sink.ReceiveFrom("sink", ad)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()

	aggregator.Start()
	p.Start(c, cancel)
}
