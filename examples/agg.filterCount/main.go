package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/core/transforms/do"

	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/aggregates"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"github.com/raralabs/canal/utils/cast"
)

const TmpPath = "./tmp/"

func main() {
	if err := os.MkdirAll(TmpPath, os.ModePerm); err != nil {
		log.Panic(err)
	}

	p := pipeline.NewPipeline(1)
	opts := pipeline.DefaultProcessorOptions

	//filename := TmpPath + "users.csv"
	var DefaultChoices = map[string][]interface{}{
		"first_name":   {"Madhav", "Shambhu", "Pushpa", "Kumar", "Hero"},
		"last_name":    {"Mashima", "Dahal", "Poudel", "Rimal", "Amatya", "Shrestha", "Bajracharya"},
		"age":          {10, 20, 30, 40, 50, 60, 70, 15, 25, 35, 45, 55, 65, 75, 100, 6, 33, 47},
		"threshold":    {20, 30, 40, 50, 60, 70, 80, 90},

	}

	src := p.AddSource("CSV Reader")
	//sp := src.AddProcessor(opts, sources.NewFileReader(filename, "age", -1))
	sp := src.AddProcessor(opts, sources.NewFaker(10,DefaultChoices))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(opts, doFn.DelayFunction(10*time.Millisecond), "path1")

	ageFilter := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()

		if v, ok := content.Get("eof"); ok {
			if v.Val == true {
				proc.Result(m, content, nil)
				proc.Done()
				return false
			}
		}

		rawAge, _ := content.Get("age")

		if age, ok := cast.TryFloat(rawAge.Val); ok {
			if age > 30 && age < 50 {
				proc.Result(m, content, nil)
			}
		}

		return false
	}
	filt := p.AddTransform("Age Filter")
	f1 := filt.AddProcessor(opts, do.NewOperator(ageFilter), "path2")

	// Count Last Names
	count := aggregates.NewCount("Count", func(m map[string]interface{}) bool {
		return true
	})
	aggs := []agg.IAggFuncTemplate{count}
	aggregator := agg.NewAggregator(aggs, nil, "age")

	counter := p.AddTransform("Aggregator")
	cnt := counter.AddProcessor(opts, aggregator.Function(), "path3")

	snk := p.AddSink("CSV Writer")
	snk.AddProcessor(opts, sinks.NewStdoutSink(), "sink")

	delay.ReceiveFrom("path1", sp)
	filt.ReceiveFrom("path2", del)
	counter.ReceiveFrom("path3", f1)
	snk.ReceiveFrom("sink", cnt)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()

	p.Start(c, cancel)
}
