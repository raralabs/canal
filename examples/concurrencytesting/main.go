package main

import (
"context"
"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/sinks"
"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/aggregates"
"time"
)
var DefaultChoices = map[string][]interface{}{
	"first_name":   {"Madhav", "Shambhu", "Pushpa", "Kumar", "Hero"},
	"last_name":    {"Mashima", "Dahal", "Shrestha"},
	"age":          {10, 20, 30, 40, 50, 60, 70, 15, 25, 35, 45, 55, 65, 75, 100, 6, 33, 47},
	"threshold":    {20, 30, 40, 50, 60, 70, 80, 90},

}
var nextChoices = map[string][]interface{}{
	"full_name":   {"Kumar Shrestha", "Hero Bajracharya","Madhav Dahal","kumar Bajracharya"},
	"age":          {10, 20, 30, 40, 50, 60, 70, 15, 25, 35, 45, 55, 65, 75, 100, 6, 33, 47},
}


func main() {
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("dummyMessage")
	src2 := newPipeline.AddSource("dumMsg")

	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewFaker(10,DefaultChoices))
	sp2 := src2.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewFaker(10,nextChoices))

	trans1 := newPipeline.AddTransform("transform")
	trans2 := newPipeline.AddTransform("transform2")

	count := aggregates.NewCount("SimpleCount", func(m map[string]interface{}) bool {
		return true
	})

	avg := aggregates.NewVariance("Sample-Variance", "value", func(m map[string]interface{}) bool {
		return true
	})

	aggs := []agg.IAggFuncTemplate{avg, count}
	aggregator := agg.NewAggregator(aggs, nil)


	tp1 := trans1.AddProcessor(pipeline.DefaultProcessorOptions, aggregator.Function(), "s1")
	tp2 := trans2.AddProcessor(pipeline.DefaultProcessorOptions,aggregator.Function(),"s2")

	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	trans1.ReceiveFrom("s1",sp1)
	trans2.ReceiveFrom("s2",sp2)
	sink.ReceiveFrom("sink",tp1)
	sink.ReceiveFrom("sink",tp2)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
