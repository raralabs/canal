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
	"github.com/raralabs/canal/core/transforms/event/poll"

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

	filename := TmpPath + "users.csv"

	src := p.AddSource("CSV Reader")
	sp := src.AddProcessor(opts, sources.NewFileReader(filename, "age", -1))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(opts, doFn.DelayFunction(10*time.Millisecond), "path1")

	ageFilter := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()

		if v, ok := content["eof"]; ok {
			if v.Val == true {
				proc.Result(m, content)
				proc.Done()
				return false
			}
		}

		rawAge := content["age"].Val
		if age, ok := cast.TryFloat(rawAge); ok {
			if age > 30 && age < 50 {
				proc.Result(m, content)
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
	aggs := []agg.IAggregator{count}
	filterEvent := poll.NewFilterEvent(func(m map[string]interface{}) bool {
		return true
	})
	after := func(m message.Msg, proc pipeline.IProcessorForExecutor, msgs []message.MsgContent) bool {
		content := m.Content()

		if v, ok := content["eof"]; ok {
			if v.Val == true {
				for _, msg := range msgs {
					proc.Result(m, msg)
				}
				return true
			}
		}
		return false
	}
	aggregator := agg.NewAggregator(filterEvent, aggs, after, "age")

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
	aggregator.Start()
	p.Start(c, cancel)
}
