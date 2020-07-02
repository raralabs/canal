package main

import (
	"context"
	"sync"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms"
	"github.com/raralabs/canal/ext/transforms/base_transforms"
)

func main() {
	p := pipeline.NewPipeline(1)

	src1 := p.AddSource("FirstSource")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(3))

	src2 := p.AddSource("SecondSource")
	sp2 := src2.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(5))

	filter1 := p.AddTransform("FirstPass")
	filter1.ReceiveFrom("f1p1", sp1)
	filter1.ReceiveFrom("f1p2", sp2)
	f1 := filter1.AddProcessor(pipeline.DefaultProcessorOptions, transforms.PassFunction(), "")

	ef := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		val := m.Content()["value"]
		if val.ValType == message.INT {
			v := val.Val.(uint64)
			if v%2 == 0 {
				proc.Result(m, m.Content())
			}
		}

		return true
	}

	filter2 := p.AddTransform("SecondPass")
	filter2.ReceiveFrom("f2p1", f1)
	f2 := filter2.AddProcessor(pipeline.DefaultProcessorOptions, base_transforms.NewDoOperator(ef), "")

	sink := p.AddSink("Sink")
	sink.ReceiveFrom("sink", f2)
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "")

	p.Validate()

	wg := sync.WaitGroup{}
	wg.Add(1)

	p.Start(context.Background(), wg.Done)
	wg.Wait()
}
