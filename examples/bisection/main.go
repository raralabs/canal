package main

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"

	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/utils/cast"
)

func main() {
	f := func(x float64) float64 {
		return x*x - 4
	}

	reqmt := map[string]interface{}{
		"lowerEnd": 0,
		"upperEnd": 12,
		"tol":      1e-10,
		"maxIters": uint64(100),
	}

	p := pipeline.NewPipeline(1)

	src := p.AddSource("Source")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewMapValueSource(reqmt, 1))

	pf := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		time.Sleep(200 * time.Millisecond)
		proc.Result(m, m.Content())
		return true
	}

	pass1 := p.AddTransform("FirstPass")
	p1 := pass1.AddProcessor(pipeline.DefaultProcessorOptions, do.NewOperator(pf), "path1")

	iter := uint64(0)
	bisect := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()

		a := content["lowerEnd"].Value()
		b := content["upperEnd"].Value()
		t := content["tol"].Value()

		af, _ := cast.TryFloat(a)
		bf, _ := cast.TryFloat(b)
		tol, _ := cast.TryFloat(t)

		fa := f(af)
		fb := f(bf)

		if fa*fb > 0 && fa*fb > tol {
			proc.Done()
			return true
		}

		c := (af + bf) / 2.0
		fc := f(c)
		if math.Abs(fc) < tol {
			content.AddMessageValue("root", message.NewFieldValue(c, message.FLOAT))
			proc.Result(m, content)
			proc.Done()
			return true
		}

		if math.Signbit(fc) == math.Signbit(fa) {
			a = c
		} else {
			b = c
		}
		content.AddMessageValue("lowerEnd", message.NewFieldValue(a, message.FLOAT))
		content.AddMessageValue("upperEnd", message.NewFieldValue(b, message.FLOAT))

		atomic.AddUint64(&iter, 1)
		it := content["maxIters"].Value()
		if iter > it.(uint64) {
			content.AddMessageValue("error", message.NewFieldValue("Iterations over before result", message.STRING))
			content.AddMessageValue("bestRoot", message.NewFieldValue(c, message.FLOAT))
			proc.Result(m, content)
			proc.Done()
			return true
		}

		proc.Result(m, content)
		return true
	}

	bisector := p.AddTransform("Bisector")
	bs := bisector.AddProcessor(pipeline.DefaultProcessorOptions, do.NewOperator(bisect), "path")

	filter := p.AddTransform("Filter Info")
	f1 := filter.AddProcessor(pipeline.DefaultProcessorOptions, do.NewOperator(func(msg message.Msg, proc pipeline.IProcessorForExecutor) bool {
		msgContent := msg.Content()
		content := make(message.MsgContent)

		uniqueInfo := []string{
			"root", "error", "bestRoot",
		}

		for _, info := range uniqueInfo {
			if value, ok := msgContent[info]; ok {
				content.AddMessageValue(info, value)
			}
		}
		content.AddMessageValue("lowerEnd", msgContent["lowerEnd"])
		content.AddMessageValue("upperEnd", msgContent["upperEnd"])

		proc.Result(msg, content)

		return true
	}), "infoPath")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	pass1.ReceiveFrom("path1", sp, bs)
	bisector.ReceiveFrom("path", p1)
	filter.ReceiveFrom("infoPath", bs)
	sink.ReceiveFrom("sink", f1)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()
	p.Start(c, cancel)

	_ = f
}
