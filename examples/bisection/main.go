package main

import (
	"context"
	content2 "github.com/raralabs/canal/core/message/content"
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
		proc.Result(m, m.Content(), nil)
		return true
	}

	pass1 := p.AddTransform("FirstPass")
	p1 := pass1.AddProcessor(pipeline.DefaultProcessorOptions, do.NewOperator(pf), "path1")

	iter := uint64(0)
	bisect := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()

		a1, _ := content.Get("lowerEnd")
		b1, _ := content.Get("upperEnd")
		t1, _ := content.Get("tol")

		a := a1.Value()
		b := b1.Value()
		t := t1.Value()

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
			content = content.Add("root", content2.NewFieldValue(c, content2.FLOAT))
			proc.Result(m, content, nil)
			proc.Done()
			return true
		}

		if math.Signbit(fc) == math.Signbit(fa) {
			a = c
		} else {
			b = c
		}
		content = content.Add("lowerEnd", content2.NewFieldValue(a, content2.FLOAT))
		content = content.Add("upperEnd", content2.NewFieldValue(b, content2.FLOAT))

		atomic.AddUint64(&iter, 1)
		it, _ := content.Get("maxIters")
		if iter > it.Value().(uint64) {
			content = content.Add("error", content2.NewFieldValue("Iterations over before result", content2.STRING))
			content = content.Add("bestRoot", content2.NewFieldValue(c, content2.FLOAT))
			proc.Result(m, content, nil)
			proc.Done()
			return true
		}

		proc.Result(m, content, nil)
		return true
	}

	bisector := p.AddTransform("Bisector")
	bs := bisector.AddProcessor(pipeline.DefaultProcessorOptions, do.NewOperator(bisect), "path")

	filter := p.AddTransform("Filter Info")
	f1 := filter.AddProcessor(pipeline.DefaultProcessorOptions, do.NewOperator(func(msg message.Msg, proc pipeline.IProcessorForExecutor) bool {
		msgContent := msg.Content()
		content := content2.New()

		uniqueInfo := []string{
			"root", "error", "bestRoot",
		}

		for _, info := range uniqueInfo {
			if value, ok := msgContent.Get(info); ok {
				content = content.Add(info, value)
			}
		}
		lower, _ := msgContent.Get("lowerEnd")
		upper, _ := msgContent.Get("upperEnd")
		content = content.Add("lowerEnd", lower)
		content = content.Add("upperEnd", upper)

		proc.Result(msg, content, nil)

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
