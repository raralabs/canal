package main

import (
	//"canal/ext/transforms/joinUsingHshMap"
	//"canal/ext/transforms"
	"github.com/raralabs/canal/ext/transforms"
	"github.com/raralabs/canal/ext/transforms/joinUsingHshMap"
	"context"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"time"
)
var DefaultChoices = map[string][]interface{}{
	"first_name":   {"Madhav", "Shambhu", "Pushpa", "Kumar", "Hero"},
	"last_name":    {"Mashima", "Dahal", "Poudel", "Rimal", "Amatya", "Shrestha", "Bajracharya"},
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

	delay1 := newPipeline.AddTransform("Delay")
	f1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	delay2 := newPipeline.AddTransform("Delay")
	f2 := delay2.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path2")
	query := "SELECT * FROM Stream1 INNERJOIN Stream2 ON Stream1.age,Stream1.first_name,Stream1. last_name = Stream2.age,Stream2.full_name"
	joinStage := newPipeline.AddTransform("join")
	j1 := joinStage.AddProcessor(pipeline.DefaultProcessorOptions,transforms.NewJoinProcessor("innerjoin",joinUsingHshMap.NewInnerJoin(joinUsingHshMap.HASH),query),"path3","path4")
	delay1.ReceiveFrom("path1", sp1)
	delay2.ReceiveFrom("path2",sp2)
	joinStage.ReceiveFrom("path3",f1)
	joinStage.ReceiveFrom("path4",f2)
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	//sink.ReceiveFrom("sink", sp1)
	sink.ReceiveFrom("sink",j1)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
