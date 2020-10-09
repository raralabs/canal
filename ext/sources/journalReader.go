package sources

import (
	"github.com/raralabs/canal/utils/regparser"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"io/ioutil"
	"regexp"
)


type JournalReader struct {
	name    		string
	blockExpression	string
	content			string
	file 			string
	maxRows  		int
	currRow			int
	blockRegEx		string
	ruleMap			map[string]string

}

func NewJournalReader(file string, maxRows int) *JournalReader {
	blockRegEx := `((?:Trx Started|Card Inserted OK|EMV FLOW)(?:.*\n)*?.*?Remaining Cash Units Counts(?:.*\n)*?.*?(?:Trx End:CardRemoved|-Wait For Card-|Card Read OK))`
	fileContent,err := ioutil.ReadFile(file)
	if err!=nil{
		panic ("error reading the file")
	}
	return &JournalReader{
		file : file,
		maxRows: maxRows,
		blockRegEx:blockRegEx,
		content: string(fileContent),
	}
}

func (jrnl *JournalReader) Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	m := messagePod.Msg
	var blocks [][]string
	if jrnl.currRow == jrnl.maxRows {
		jrnl.done(m, proc)
		return false
	}
	cardNumberRule := `(?:Trx Started|Card Read OK|EMV FLOW|Menu Button Enabled)(?:.*\n)*?.*?Card Number\s+:\s+(?P<card_no>\d+.*?\d+)`
	regCard,err := regexp.Compile(cardNumberRule)
	if err!=nil{

		panic ("could not compile regex for card number")
	}
	regRule:=`(?P<txn_type>Fast Cash|Cash Withdraw Initiated|Cardless Cash Request|Fund Transfer)(?:.*\n)*?.*?(?:AUX NO|Aux Number)\s+:\s+(?:\:.*\:)?(?P<terminal_id>\w{8})(?:.*\n)*?.*?\[(?P<txn_date>\d+).*?(?:Fast Cash Completed OK|Withdraw Status : OK|Cardless Cash Request Successful|Fund Transfer Completed)(?:(?:.*\n)*?.*?(?:Account\s+:\s+(?P<account_no>\d*)|)(?:.*\n)*?.*?)?Trace ID\s+:\s+(?P<trace_id>\d+|)(?:(?:.*\n)*?.*?(?:TRX NO\s+:\s+(?P<trx_no>\w*|)(?:.*\n)*?.*?)?Dispense Command Executed(?:.*\n)*?.*?Amount\s+:\s+(?P<txn_amount>\d+)(?:.*\n)*?.*?Currency\s+:\s+(?P<currency>\w+))`
	regFields,err2 := regexp.Compile(regRule)
	if err2 != nil{
		panic ("could not compile regex for fields")
	}
	regExp:= regexp.MustCompile(jrnl.blockRegEx)
	blocks = regExp.FindAllStringSubmatch(jrnl.content,-1)
	for _,block := range blocks{
		msgContent := content.New()
		cardNumberHolder := regparser.ExtractParams(regCard,block[0])
		fieldHolder := regparser.ExtractParams(regFields,block[0])
		msgContent.Add("card_no",content.NewFieldValue(cardNumberHolder["card_no"],content.STRING))

		for key,value := range fieldHolder{
			if key == "Deno1"||key=="Deno2"||key=="Count1"||key=="Count2"||key=="Reject1"||key=="Reject2"||key=="Total Count1"||key=="Total Count2"||key=="txn_amount"{
				msgContent.Add(key,content.NewFieldValue(value,content.FLOAT))
			}else{
				if key =="txn_date"{
					mm := value [:2]
					dd := value [2:4]
					yyyy := value [4:]
					date := yyyy+"-"+mm+"-"+dd
					msgContent.Add(key,content.NewFieldValue(date,content.STRING))
				}else{
					msgContent.Add(key,content.NewFieldValue(value,content.STRING))
				}

			}

		}
		proc.Result(m,msgContent,nil)
	}
	jrnl.done(m,proc)
	jrnl.currRow++
	return false
}

func (jrnl *JournalReader) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	contents := content.New()
	contents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, contents, nil)
	proc.Done()
}

func (jrnl *JournalReader) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (jrnl *JournalReader) HasLocalState() bool {
	return false
}

func (jrnl *JournalReader) SetName(name string) {
	jrnl.name = name
}

func (jrnl *JournalReader) Name() string {
	return jrnl.name
}



