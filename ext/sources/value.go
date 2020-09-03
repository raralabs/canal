package sources

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/utils/extract"
)

type MapValue struct {
	name string // The name of the source

	values content.IContent // The values to be passed
	times  int              // The number of times the values should be passed
}

func preprocess(m map[string]interface{}) content.IContent {

	mVal := content.New()

	for k, v := range m {
		val, valType := extract.ValType(v)
		mVal = mVal.Add(k, content.NewFieldValue(val, valType))
	}

	return mVal
}

func NewMapValueSource(val map[string]interface{}, times int) pipeline.Executor {

	mv := &MapValue{}
	mv.values = content.New()

	for k, v := range val {
		value, valType := extract.ValType(v)
		mv.values = mv.values.Add(k, content.NewFieldValue(value, valType))
	}

	mv.times = times

	return mv
}

func (mv *MapValue) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (mv *MapValue) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

	if mv.times != 0 {
		times := mv.times
		if mv.times == -1 {
			times = 1
		}

		if times > 0 {

			// Decrement times counter
			if mv.times != -1 {
				mv.times--
			}

			proc.Result(m, mv.values, nil)
		}
	} else {
		proc.Done()
	}

	return true
}

func (mv *MapValue) HasLocalState() bool {
	return false
}

func (mv *MapValue) SetName(name string) {
	mv.name = name
}

func (mv *MapValue) Name() string {
	return mv.name
}
