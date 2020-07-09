package main

import (
	"context"
	"encoding/binary"
	"log"
	"time"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"

	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
)

func main() {

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewInlineRange(100))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")

	bucket := 10
	count := 0
	add := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()
		vl, _ := content.Get("value")
		v := vl.Val
		sum, _ := v.(uint64)

		db := proc.Persistor()
		if db == nil {
			log.Panic("Need persistor but none available!!")
		}
		val, err := db.Get("sum")
		if err == nil {
			sum += binary.BigEndian.Uint64(val)
		}

		number := make([]byte, 8)
		binary.BigEndian.PutUint64(number, sum)
		db.Store("sum", number)

		count++
		if count >= bucket {
			count = 0
			msgContent := message.NewOrderedContent()
			msgContent.Add("sum", message.NewFieldValue(sum, message.INT))
			proc.Result(m, msgContent)
		}
		return true
	}

	adder := p.AddTransform("Adder")
	opts := pipeline.DefaultProcessorOptions
	opts.Persistor = true
	ad := adder.AddProcessor(opts, do.NewOperator(add), "path")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	delay.ReceiveFrom("path1", sp)
	adder.ReceiveFrom("path", del)
	sink.ReceiveFrom("sink", ad)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()
	p.Start(c, cancel)
}
