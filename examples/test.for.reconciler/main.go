package main
import (
	"bufio"
	"github.com/Knetic/govaluate"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/ext/transforms"
	"regexp"
	"github.com/raralabs/canal/utils/regparser"
	"context"
	//"regexp"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"log"
	"os"
	"os/exec"
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
	dir := strings.TrimSpace(string(cmdOut)) + "/examples/test.for.reconciler/"
	readFile := dir + "empinfo.csv"
	secReadFile := dir +"tax.csv"
	file, err := os.Open(readFile)
	secFile,_:= os.Open(secReadFile)
	r := bufio.NewReader(file)
	r2 := bufio.NewReader(secFile)
	newPipeline:= pipeline.NewPipeline(1)
	src1 := newPipeline.AddSource("File Reader1")
	sp1 := src1.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewCsvReader(r, -1))
	src2 := newPipeline.AddSource("File Reader2")
	sp2 := src2.AddProcessor(pipeline.DefaultProcessorOptions, sources.NewCsvReader(r2, -1))
	delay1 := newPipeline.AddTransform("Delay1")
	d1 := delay1.AddProcessor(pipeline.DefaultProcessorOptions, doFn.DelayFunction(100*time.Millisecond), "path1")
	delay2 := newPipeline.AddTransform("Delay2")
	d2 := delay2.AddProcessor(pipeline.DefaultProcessorOptions,doFn.DelayFunction(100*time.Millisecond),"path2")
	filter := newPipeline.AddTransform("regex fiter")
	f1 := filter.AddProcessor(pipeline.DefaultProcessorOptions,
		doFn.RegexValidator(`\d{10}`,"phone ",
			func(reg *regexp.Regexp, str string)bool{
				matched :=regparser.ValidateData(reg,str)
				return matched
			}),
		"path3")

	enricher := newPipeline.AddTransform("enrich data")
	evaluableExp,_ := govaluate.NewEvaluableExpression("first_name+' '+last_name")
	e1 := enricher.AddProcessor(pipeline.DefaultProcessorOptions,doFn.EnrichFunction("full_name",evaluableExp,func(m message.Msg)bool{
		if m.Content().Keys()[0] == "eof"{
		return true
	}else{
		return false
		}
	}),"path0")
	joiner := newPipeline.AddTransform("innerJoin")
	query := "SELECT id,phone ,first_name,full_name,age,tax FROM path4 c INNERJOIN path5 d on path4.emp_id = path5.id"
	j1 := joiner.AddProcessor(pipeline.DefaultProcessorOptions,transforms.NewJoinProcessor("outerjoin",query),"path4","path5")

	//enricher := newPipeline.AddTransform("enricher")
	//e1 := enricher.AddProcessor(pipeline.DefaultProcessorOptions,doFn.EnrichFunction())
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	delay1.ReceiveFrom("path1", sp1)
	delay2.ReceiveFrom("path2",sp2)
	filter.ReceiveFrom("path3",d1)
	//for enrichment
	enricher.ReceiveFrom("path0",f1)

	//for inner join
	joiner.ReceiveFrom("path4",e1)
	joiner.ReceiveFrom("path5",d2)

	sink.ReceiveFrom("sink",j1)
	//sink2.ReceiveFrom("sink2",d2)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}