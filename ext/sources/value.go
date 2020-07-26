package sources

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/utils/cast"
)

type MapValue struct {
	name string // The name of the source

	values *message.OrderedContent // The values to be passed
	times  int                     // The number of times the values should be passed
}

func preprocess(m map[string]interface{}) *message.OrderedContent {

	mVal := message.NewOrderedContent()

	for k, v := range m {
		val, valType := cast.ValType(v)
		mVal.Add(k, message.NewFieldValue(val, valType))
	}

	return mVal
}

func NewMapValueSource(val map[string]interface{}, times int) pipeline.Executor {

	mv := &MapValue{}
	mv.values = message.NewOrderedContent()

	for k, v := range val {
		value, valType := cast.ValType(v)
		mv.values.Add(k, message.NewFieldValue(value, valType))
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
