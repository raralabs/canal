package main

import (
	"context"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/sinks"
	"github.com/raralabs/canal/sources"
	"github.com/raralabs/canal/transforms"
	"time"
)

func main() {
	p := pipeline.NewPipeline(1)

	src := p.AddSource("FirstSource")
	sp := src.AddProcessor(sources.NewInlineRange(2))

	filter1 := p.AddTransform("FirstPass")
	f1 := filter1.AddProcessor(transforms.PassFunction(), "path1")

	filter2 := p.AddTransform("SecondPass")
	f2 := filter2.AddProcessor(transforms.PassFunction(), "path2")

	sink := p.AddSink("Sink")
	sink.AddProcessor(sinks.NewStdoutSink(), "sink")

	filter1.ReceiveFrom("path1", sp, f2)
	filter2.ReceiveFrom("path2", f1)
	sink.ReceiveFrom("sink", f2)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()
	p.Start(c, cancel)
}
