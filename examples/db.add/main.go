package main

import (
	"context"
	"encoding/binary"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/sinks"
	"github.com/raralabs/canal/sources"
	"github.com/raralabs/canal/transforms"
	"github.com/raralabs/canal/transforms/base_transforms"
	"time"
)

func main() {

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Source")
	sp := src.AddProcessor(sources.NewInlineRange(100))

	delay := p.AddTransform("Delay")
	del := delay.AddProcessor(transforms.DelayFunction(100*time.Millisecond), "path1")

	bucket := 10
	count := 0
	add := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()
		v := content["value"].Val
		sum, _ := v.(uint64)

		db := proc.Persistor()
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
			msgContent := message.MsgContent{
				"sum": message.NewFieldValue(sum, message.INT),
			}
			proc.Result(m, msgContent)
		}
		return true
	}

	adder := p.AddTransform("Adder")
	ad := adder.AddProcessor(base_transforms.NewDoOperator(add), "path")

	sink := p.AddSink("Sink")
	sink.AddProcessor(sinks.NewStdoutSink(), "sink")

	delay.ReceiveFrom("path1", sp)
	adder.ReceiveFrom("path", del)
	sink.ReceiveFrom("sink", ad)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()
	p.Start(c, cancel)
}
