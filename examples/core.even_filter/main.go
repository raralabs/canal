package main

import (
	"context"
	"github.com/n-is/canal/core/pipeline"
	"github.com/n-is/canal/sinks"
	"github.com/n-is/canal/sources"
	"github.com/n-is/canal/transforms"
	"sync"
)

func main() {
	p := pipeline.NewPipeline(1)

	src1 := p.AddSource("FirstSource")
	sp1 := src1.AddProcessor(sources.NewInlineRange(5))

	src2 := p.AddSource("SecondSource")
	sp2 := src2.AddProcessor(sources.NewInlineRange(15))

	filter1 := p.AddTransform("FirstPass")
	filter1.ReceiveFrom("f1p1", sp1)
	filter1.ReceiveFrom("f1p2", sp2)
	f1 := filter1.AddProcessor(transforms.PassFunction())

	filter2 := p.AddTransform("SecondPass")
	filter2.ReceiveFrom("f2p1", f1)
	f2 := filter2.AddProcessor(transforms.PassFunction())

	sink := p.AddSink("Sink")
	sink.ReceiveFrom("sink", f2)
	sink.AddProcessor(sinks.NewBlackholeSink())

	p.Validate()

	wg := sync.WaitGroup{}
	wg.Add(1)

	p.Start(context.Background(), wg.Done)
	wg.Wait()
}
