package main

import (
	"context"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"time"

	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
)

func main() {
	p := pipeline.NewPipeline(1)

	src := p.AddSource("FirstSource")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(2))

	filter1 := p.AddTransform("FirstPass")
	f1 := filter1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.PassFunction(), "path1")

	filter2 := p.AddTransform("SecondPass")
	f2 := filter2.AddProcessor(pipeline.DefaultProcessorOptions, doFn.PassFunction(), "path2")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	filter1.ReceiveFrom("path1", sp, f2)
	filter2.ReceiveFrom("path2", f1)
	sink.ReceiveFrom("sink", f2)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()
	p.Start(c, cancel)
}
