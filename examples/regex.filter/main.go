package main

import (
	"context"
	"github.com/raralabs/canal/ext/transforms/doFn"
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
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewFileReader(readFile, "Phone Num", -1))
	delay := newPipeline.AddTransform("Delay")
	f1 := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	regexFilter := newPipeline.AddTransform("regexFilter")
	m1 := regexFilter.AddProcessor(pipeline.DefaultProcessorOptions,
		doFn.RegExp(`\(?\+?\d{0,3}\)?\-?\d{10}`, "Phone Num", func(reg *regexp.Regexp, str string) string{
			s := reg.MatchString(str)
			if s==true{
				return str
			}
			return "invalid number"
		}),
		"path2")

	validated := newPipeline.AddTransform("Proper Phone Number")
	v1:=validated.AddProcessor(pipeline.DefaultProcessorOptions,doFn.PassFunction(),"path3")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay.ReceiveFrom("path1", sp)
	regexFilter.ReceiveFrom("path2", f1)
	validated.ReceiveFrom("path3",m1)
	sink.ReceiveFrom("sink", v1)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
