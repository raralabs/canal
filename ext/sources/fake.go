package sources

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/utils/extract"
	"github.com/raralabs/canal/utils/faker"
)

type Fake struct {
	name    string
	maxRows int64
	currRow int64
	faker   *faker.Stream
	random  map[string]interface{}
}
//utilizes the function of utils faker and produces dummy data
func NewFaker(maxRows int64, m map[string][]interface{}) *Fake {

	if m == nil {
		m = faker.DefaultChoices
	}

	return &Fake{
		name:    "Fake",
		maxRows: maxRows,
		currRow: 0,
		faker:   faker.NewFake(m),
		random:  make(map[string]interface{}),
	}
}

//func (f *Fake) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
func (f *Fake) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {
	if f.currRow >= f.maxRows {
		f.done(m.Msg, proc)
		return false
	}

	contents := content.New()
	f.faker.Random(f.random)

	for k, v := range f.random {
		value, valType := extract.ValType(v)
		contents = contents.Add(k, content.NewFieldValue(value, valType))
	}
	proc.Result(m.Msg, contents, nil)
	f.currRow++

	return false
}

func (f *Fake) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	contents := content.New()
	contents = contents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, contents, nil)
	proc.Done()
}

func (f *Fake) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}


func (f *Fake) HasLocalState() bool {
	return false
}

func (f *Fake) SetName(name string) {
	f.name = name
}

func (f *Fake) Name() string {
	return f.name
}
