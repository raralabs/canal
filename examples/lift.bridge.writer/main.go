package main

import "fmt"

func main() {
	fmt.Println("Examples are failing")
}

//! These tests fails due to syntax error
/*
func main() {
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("liftBridgeWriter")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(10))
	delay1 := newPipeline.AddTransform("Delay1")
	d1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewLiftBridgeWriter("su","subodh.chandra",9292), "sink")
	delay1.ReceiveFrom("path1", sp1)
	sink.ReceiveFrom("sink",d1)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
*/
