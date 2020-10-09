package sources

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"github.com/raralabs/canal/core/message/content"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type data struct{
	Transaction_date 	string  `json:"transaction_date"`
	Account_no		 	string	`json:"account_no"`
	Trace_id			string	`json:"trace_id"`
	Transaction_amount 	string	`json:"transaction_amount"`
	Card_no				string	`json:"card_no"`

}

type JsonReader struct {
	name    string
	Data 	[]data
	file 	*os.File
	maxRows  int
	currRow  int
}

func NewJsonReader(file *os.File, maxRows int) *JsonReader {
	return &JsonReader{
		file : file,
		maxRows: maxRows,
		currRow: 0,
	}
}

func (jr *JsonReader) Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	m := messagePod.Msg
	if jr.currRow == jr.maxRows {
		jr.done(m, proc)
		return false
	}

	byteValue, _ := ioutil.ReadAll(jr.file)
	err:= json.Unmarshal(byteValue, &jr.Data)

	if err != nil {

		jr.done(m, proc)
		return false
	}


	for _,data :=range jr.Data {
		contents := content.New()
		contents.Add("transaction_date",content.NewFieldValue(data.Transaction_date,content.STRING))
		contents.Add("account_no",content.NewFieldValue(data.Account_no,content.STRING))
		contents.Add("trace_id",content.NewFieldValue(data.Trace_id,content.STRING))
		contents.Add("transaction_amount",content.NewFieldValue(data.Transaction_amount,content.STRING))
		contents.Add("card_no",content.NewFieldValue(data.Card_no,content.STRING))

		proc.Result(m,contents,nil)
	}
	jr.currRow++
	return false
}

func (jr *JsonReader) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	contents := content.New()
	contents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, contents, nil)
	proc.Done()
}

func (jr *JsonReader) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (jr *JsonReader) HasLocalState() bool {
	return false
}

func (jr *JsonReader) SetName(name string) {
	jr.name = name
}

func (jr *JsonReader) Name() string {
	return jr.name
}

