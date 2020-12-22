

package main

import (
	"context"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/aggregates"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"github.com/raralabs/canal/plugins/transform"
	"time"
)


func main() {
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("liftBridgeReader")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewLiftBridgeReader("pgservice-stream",sources.RECENT,time.Now(),int64(1),9292))
	delay1 := newPipeline.AddTransform("Delay1")
	d1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	tp1 := newPipeline.AddTransform("transform1")
	t1 := tp1.AddProcessor(pipeline.DefaultProcessorOptions,transform.NewErpDataGetter("dataformatter"),"path2")
	activeCount := aggregates.NewCount("active count", func(m map[string]interface{}) bool{
		if m["Data"].(float64)>=51{
			return true
		}else{
			return false
		}
	})
	inactiveCount := aggregates.NewCount("inactive count",func(m map[string]interface{})bool{
		if m["Data"].(float64)<=50{
			return true
		}else{
			return false
		}
	})
	aggs := []agg.IAggFuncTemplate{activeCount,inactiveCount}
	aggregator := agg.NewAggregator(aggs, nil)

	counter := newPipeline.AddTransform("Adder")
	ad := counter.AddProcessor(pipeline.DefaultProcessorOptions, aggregator.Function(), "path")

	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay1.ReceiveFrom("path1",sp1)
	tp1.ReceiveFrom("path2",d1)
	counter.ReceiveFrom("path", t1)
	sink.ReceiveFrom("sink", ad)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()

	newPipeline.Start(c, cancel)


}


