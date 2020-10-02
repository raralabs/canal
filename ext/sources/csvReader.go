package sources

import (
	"encoding/csv"
	"fmt"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/utils/extract"
	"io"
	"log"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type CsvReader struct {
	name    string
	reader  *csv.Reader
	maxRows int
	currRow int
	header  []string
}

func NewCsvReader(r io.Reader, maxRows int) *CsvReader {

	csr := csv.NewReader(r)
	csr.TrimLeadingSpace = true

	header, err := csr.Read()

	if err != nil { //read header
		fmt.Println("err",err)
		log.Panic(err)
	}

	return &CsvReader{
		reader:  csr,
		maxRows: maxRows,
		currRow: 0,
		header:  header,
	}
}

func (cr *CsvReader) Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	m := messagePod.Msg
	if cr.currRow == cr.maxRows {
		cr.done(m, proc)
		return false
	}

	record, err := cr.reader.Read()
	if err == io.EOF {
		cr.done(m, proc)
		return false
	} else if err != nil {
		log.Panic(err)
	}

	if len(record) != len(cr.header) {
		log.Panic("Record and Header Length Mismatch")
	}

	contents := content.New()
	for i, v := range cr.header {
		value, valType := extract.ValType(record[i])
		contents.Add(v, content.NewFieldValue(value, valType))
	}

	proc.Result(m, contents, nil)
	cr.currRow++
	return false
}

func (cr *CsvReader) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	contents := content.New()
	contents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, contents, nil)
	proc.Done()
}

func (cr *CsvReader) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (cr *CsvReader) HasLocalState() bool {
	return false
}

func (cr *CsvReader) SetName(name string) {
	cr.name = name
}

func (cr *CsvReader) Name() string {
	return cr.name
}