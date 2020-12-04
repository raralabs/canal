package main

import (
	"context"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"time"
)

func main() {
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("liftBridgeReader")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewLiftBridgeReader("foo-stream",sources.OLDEST,time.Now(),int64(1),9292))
	delay1 := newPipeline.AddTransform("Delay1")
	d1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay1.ReceiveFrom("path1", sp1)
	sink.ReceiveFrom("sink",d1)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}

