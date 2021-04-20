package main

//! These tests fails due to mismatch function call
/*
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

	journalValidator := newPipeline.AddTransform("journalValidator")
	journalValidator_obj := doFn.NewJournalValidator()
	j1 := journalValidator.AddProcessor(pipeline.DefaultProcessorOptions,journalValidator_obj.JournalFieldValidator(
		"card_no","txn_date","account_no","terminal_id","status","trace_id","trace_id1",
		"count1","count2", "txn_type","trx_no","txn_amount","currency","response","deno1",
		"deno2","reject1","reject2","total_count1","total_count2"),"path9")



	functions := map[string]govaluate.ExpressionFunction{
		"extractCardNum":func(args ...interface{})(interface{},error){
			var class map[string]string
			regRule := `\d{1,6}[X|*]+\d{1}?(?P<lastdigits>\d{3})$`
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

	enricher2 := newPipeline.AddTransform("secondEnricher")
	e2 := enricher2.AddProcessor(pipeline.DefaultProcessorOptions,doFn.EnrichFunction("CardLastDigits",expression,func(m message.Msg)bool{
		if m.Content().Keys()[0] == "eof"{
			return true
		}else{
			return false
		}
	}),"pathx")

	joiner := newPipeline.AddTransform("innerJoin")
	query := "SELECT transaction_amount,card_no,transaction_date FROM path4 c INNERJOIN path5 d on path4.CardLastDigits=path5.CardLastDigits"
	jo1 := joiner.AddProcessor(pipeline.DefaultProcessorOptions,transforms.NewJoinProcessor("outerjoin",query),"path4","path5")
	sink := newPipeline.AddSink("Sink")
	sink.AddProcessor(pipeline.DefaultProcessorOptions, sinks.NewStdoutSink(), "sink")
	delay1.ReceiveFrom("path1", sp1)
	delay2.ReceiveFrom("path0",sp2)
	ledgervalidator.ReceiveFrom("path2",d1)
	journalValidator.ReceiveFrom("path9",d2)
	enricher.ReceiveFrom("path3",v1)
	enricher2.ReceiveFrom("pathx",j1)
	joiner.ReceiveFrom("path4",e1)
	joiner.ReceiveFrom("path5",e2)

	sink.ReceiveFrom("sink",jo1)
	c, cancel := context.WithTimeout(context.Background(), 1000*time.Second)
	newPipeline.Validate()
	newPipeline.Start(c, cancel)
}
*/
