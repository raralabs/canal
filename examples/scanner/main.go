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

	dir := strings.TrimSpace(string(cmdOut)) + "/examples/scanner/"
	readFile := dir + "names.txt"
	writeFile := dir + "newNames.txt"

	p := pipeline.NewPipeline(1)

	src := p.AddSource("File Reader")
	sp := src.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewFileReader(readFile, "name", -1))

	delay := p.AddTransform("Delay")
	f1 := delay.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")

	middleNameRemover := p.AddTransform("Middle Names Remover")
	m1 := middleNameRemover.AddProcessor(pipeline.DefaultProcessorOptions,
		doFn.RegExp(`([a-zA-Z]+) (.*) ([a-zA-Z]+)`, "name", func(reg *regexp.Regexp, str string) string {
			s := string(reg.ReplaceAll([]byte(str), []byte("${1} ${3}")))
			return s
		}),
		"path2")

	sink := p.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewFileWriter(writeFile, "name"), "sink")

	delay.ReceiveFrom("path1", sp)
	middleNameRemover.ReceiveFrom("path2", f1)
	sink.ReceiveFrom("sink", m1)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	p.Validate()
	p.Start(c, cancel)
}
