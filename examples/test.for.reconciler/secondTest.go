package main
import (
	"bufio"
	"context"
	"github.com/Knetic/govaluate"
	"regexp"

	//"fmt"
	//"github.com/Knetic/govaluate"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/ext/transforms"
	"github.com/raralabs/canal/utils/regparser"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/sinks"
	"github.com/raralabs/canal/ext/sources"
	"github.com/raralabs/canal/ext/transforms/doFn"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

func main() {
	cmdOut, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()

	if err != nil {
		log.Panicf("Could not read source folder through git")
	}
	dir := strings.TrimSpace(string(cmdOut)) + "/examples/test.for.reconciler/"
	readFile := dir + "data1.csv"
	secReadFile := dir +"data2.csv"
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
		doFn.RegexValidator(`[A-Za-z]+[-]\d{5}$`,"trans_description",
			func(reg *regexp.Regexp, str string)bool{
				matched :=regparser.ValidateData(reg,str)
				return matched
			}),
		"path3")
	functions := map[string]govaluate.ExpressionFunction{
			"getTranId":func(args ...interface{})(interface{},error){
			var extractedTranId map[string]string
			reg,err := regexp.Compile(`[A-Za-z]+[-](?P<tran_id>\d{5})`)
			if err!=nil{
				panic("couldn't compile regex")
			}
			for _,arg := range args{
				extractedTranId = regparser.ExtractParams(reg,arg.(string))

			}
			return extractedTranId["tran_id"],nil
		},
	}

	expression, _ := govaluate.NewEvaluableExpressionWithFunctions("getTranId(trans_description)", functions)
	enricher := newPipeline.AddTransform("label_data")
	l1 := enricher.AddProcessor(pipeline.DefaultProcessorOptions,doFn.EnrichFunction("transaction_id",expression, func(m message.Msg) bool {
		if m.Content().Keys()[0] == "eof"{
			return true
		}else{
			return false
		}
	}),"path18")

	joiner := newPipeline.AddTransform("innerJoin")
	query := "SELECT trans_date,trans_id,amount,cr_amount FROM path4 c INNERJOIN path5 d on path4.transaction_id,path4.amount = path5.trans_id,path5.cr_amount"
	j1 := joiner.AddProcessor(pipeline.DefaultProcessorOptions,transforms.NewJoinProcessor("innerjoin",query),"path4","path5")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")

	delay1.ReceiveFrom("path1", sp1)
	delay2.ReceiveFrom("path2",sp2)
	filter.ReceiveFrom("path3",d1)
	//for enrichment
	enricher.ReceiveFrom("path18",f1)
	//enricher.ReceiveFrom("path0",l1)

	//for inner join
	joiner.ReceiveFrom("path4",l1)
	joiner.ReceiveFrom("path5",d2)

	sink.ReceiveFrom("sink",j1)
	//sink2.ReceiveFrom("sink2",d2)

	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}