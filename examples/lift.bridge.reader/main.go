package main

import (
	"context"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"github.com/raralabs/canal/plugins/transform"
	"time"
)

func main() {
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("liftBridgeReader")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewLiftBridgeReader("stream",sources.RECENT,time.Now(),int64(1),"",9292))
	delay1 := newPipeline.AddTransform("Delay1")
	d1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	tp1 := newPipeline.AddTransform("transform1")
	t1 := tp1.AddProcessor(pipeline.DefaultProcessorOptions,transform.NewErpDataGetter("dataformatter"),"path2")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay1.ReceiveFrom("path1", sp1)
	tp1.ReceiveFrom("path2",d1)
	sink.ReceiveFrom("sink",t1)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}

