package sources

import (
	"strconv"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type MapValue struct {
	name string // The name of the source

	values message.MsgContent // The values to be passed
	times  int                // The number of times the values should be passed
}

func getValType(v interface{}) (interface{}, message.FieldValueType) {
	switch val := v.(type) {
	case bool:
		return val, message.BOOL

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return val, message.INT

	case float32, float64:
		return val, message.FLOAT

	case string:
		if value, err := strconv.ParseInt(val, 10, 64); err == nil {
			return value, message.INT
		} else if value, err := strconv.ParseFloat(val, 64); err == nil {
			return value, message.FLOAT
		}

		return val, message.STRING
	}

	return v, message.NONE
}

func preprocess(m map[string]interface{}) message.MsgContent {

	mVal := make(message.MsgContent)

	for k, v := range m {
		val, valType := getValType(v)
		mVal.AddMessageValue(k, message.NewFieldValue(val, valType))
	}

	return mVal
}

func NewMapValueSource(val map[string]interface{}, times int) pipeline.Executor {

	mv := &MapValue{}
	mv.values = make(message.MsgContent)

	for k, v := range val {
		value, valType := getValType(v)
		mv.values.AddMessageValue(k, message.NewFieldValue(value, valType))
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

			proc.Result(m, mv.values)
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
