package main

import (
	"context"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"github.com/raralabs/canal/utils/regparser"
	"log"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
)

func main() {
	cmdOut, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		log.Panicf("Could not read source folder through git")
	}
	dir := strings.TrimSpace(string(cmdOut)) + "/examples/regex.filter/"
	readFile := dir + "user_info.txt"
	newPipeline:= pipeline.NewPipeline(1)
	src := newPipeline.AddSource("File Reader")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewFileReader(readFile, "userInfo", -1))
	delay := newPipeline.AddTransform("Delay")
	f1 := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	regexFilter := newPipeline.AddTransform("regexFilter")
	m1 := regexFilter.AddProcessor(pipeline.DefaultProcessorOptions,
		doFn.RegExParser(`is\s+(?P<first_name>\w+).*?am\s+(?P<age>\d+)`, "userInfo",func(reg *regexp.Regexp, str string) map[string]string {
			data := regparser.ExtractParams(reg,str)
			return data}),
		"path2")


	count := aggregates.NewCount("Count", func(m map[string]interface{}) bool {
		return true
	})
	aggs := []agg.IAggFuncTemplate{count}
	aggregator := agg.NewAggregator(aggs, nil, "first_name")

	counter := newPipeline.AddTransform("Aggregator")
	cnt := counter.AddProcessor(pipeline.DefaultProcessorOptions, aggregator.Function(), "path3")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay.ReceiveFrom("path1", sp)
	regexFilter.ReceiveFrom("path2", f1)
	counter.ReceiveFrom("path3",m1)
	sink.ReceiveFrom("sink", m1,cnt)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
