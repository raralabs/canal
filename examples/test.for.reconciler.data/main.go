package main

import (
	//"bufio"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/regparser"
	"context"
	"github.com/Knetic/govaluate"
	"os"
	"regexp"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"log"

	"os/exec"
	"strings"
	"time"
)

func main() {
	cmdOut, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()

	if err != nil {
		log.Panicf("Could not read source folder through git")
	}
	dir := strings.TrimSpace(string(cmdOut)) + "/examples/test.for.reconciler.data/"
	readFile := dir + "jsondata.json"
	file, err1 := os.Open(readFile)
	if err1!=nil{
		panic ("Could not read the ledger file")
	}
	readJrnl := dir + "journalData.jnl"
	//r := bufio.NewReader(file)
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("Json Reader")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewJsonReader(file, -1))
	src2 := newPipeline.AddSource("journal Reader")
	sp2 := src2.AddProcessor(pipeline.DefaultProcessorOptions,sources.NewJournalReader(readJrnl,-1))
	delay1 := newPipeline.AddTransform("Delay1")
	d1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	delay2 := newPipeline.AddTransform("Delay2")
	d2 := delay2.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path0")
	ledgervalidator := newPipeline.AddTransform("validator")
	ledgervalidator_obj:= doFn.NewReconValidator()
	v1:= ledgervalidator.AddProcessor(pipeline.DefaultProcessorOptions,ledgervalidator_obj.ReconFieldValidator(
		"card_no","transaction_date","account_no",
		"ft_id","auth_no","terminal_id","opening_balance",
		"closing_balance","account_name","status","trace_id"),"path2")

	functions := map[string]govaluate.ExpressionFunction{
		"extractCardNum":func(args ...interface{})(interface{},error){
			var class map[string]string
			regRule := `\d{1,6}[X]+\d{1}?(?P<lastdigits>\d{3})$`
			regEx,err := regexp.Compile(regRule)
			if err != nil{
				panic ("Could not compile regex")
			}
			for _,arg := range(args){
				class = regparser.ExtractParams(regEx,arg.(string))
			}
			return class["lastdigits"],nil
		},
	}


	enricher := newPipeline.AddTransform("Enricher")
	expression, _ := govaluate.NewEvaluableExpressionWithFunctions("extractCardNum(card_no)", functions)
	e1 := enricher.AddProcessor(pipeline.DefaultProcessorOptions,doFn.EnrichFunction("CardLastDigits",expression,func(m message.Msg)bool{
		if m.Content().Keys()[0] == "eof"{
			return true
		}else{
			return false
		}
	}),"path3")

	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay1.ReceiveFrom("path1", sp1)
	delay2.ReceiveFrom("path0",sp2)
	ledgervalidator.ReceiveFrom("path2",d1)
	enricher.ReceiveFrom("path3",v1)
	sink.ReceiveFrom("sink",e1,d2)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
