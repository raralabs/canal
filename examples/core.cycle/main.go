package main

import (
	"context"
	"github.com/n-is/canal/core/pipeline"
	"github.com/n-is/canal/sinks"
	"github.com/n-is/canal/sources"
	"github.com/n-is/canal/transforms"
	"time"
)

func main() {
	p := pipeline.NewPipeline(1)

	src := p.AddSource("FirstSource").Trace()
	sp := src.AddProcessor(sources.NewInlineRange(1))

	filter1 := p.AddTransform("FirstPass")
	f1 := filter1.AddProcessor(transforms.PassFunction(), "path1")

	filter2 := p.AddTransform("SecondPass")
	f2 := filter2.AddProcessor(transforms.PassFunction())

	sink := p.AddSink("Sink")
	sink.AddProcessor(sinks.NewStdoutSink())

	filter1.ReceiveFrom("path1", sp, f2)
	filter2.ReceiveFrom("path1", f1)
	sink.ReceiveFrom("sink", f2)

	c, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	p.Validate()
	p.Start(c, cancel)
}
