package main

import (
	"context"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"time"
)

func main() {

	pipe := pipeline.NewPipeline(1)

	src := pipe.AddSource("Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(100))

	delay := pipe.AddTransform("Delay")
	del := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")


	count:=0
	evenCounter := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		contents := m.Content()
		vl, _ := contents.Get("value")
		v := vl.Val
		if v.(uint64)%2==0{
			count++
		}
		msgContent := content.New()
		msgContent.Add("count", content.NewFieldValue(count, content.INT))
		proc.Result(m, msgContent, nil)

		return true
	}
	filter := pipe.AddTransform("EvenFilter")
	opts := pipeline.DefaultProcessorOptions
	opts.Persistor = false
	ad := filter.AddProcessor(opts, do.NewOperator(evenCounter), "path")

	sink := pipe.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	delay.ReceiveFrom("path1", sp)
	filter.ReceiveFrom("path", del)
	sink.ReceiveFrom("sink", ad)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	pipe.Validate()
	pipe.Start(c, cancel)
}

