package sources

import (
	"github.com/n-is/canal/core/message"
	"github.com/n-is/canal/core/pipeline"
	"strconv"
)

type MapValue struct {
	name string // The name of the source

	values []message.MsgContent // The values to be passed
	times  int                  // The number of times the values should be passed

	currIndex int // The current index of value to be passed
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

//func NewMapValueSource(vals []map[string]interface{}, times int) core.executor {
//
//	mv := &MapValue{}
//	mv.values = make([]core.messageContent, len(vals))
//
//	for i, val := range vals {
//		processedVal := preprocess(val)
//		mv.values[i] = processedVal
//	}
//
//	mv.times = times
//
//	return mv
//}

func (mv *MapValue) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

//func (mv *MapValue) execute(m *core.Msg) ([]*core.messageContent, bool, error) {
//
//	if mv.times != 0 && len(mv.values) > 0 {
//		times := mv.times
//		if mv.times == -1 {
//			times = 1
//		}
//
//		if times > 0 {
//			mes := mf.NewExecute(mv.values[mv.currIndex])
//
//			mv.currIndex++
//
//			// Decrement times counter
//			if mv.times != -1 && mv.currIndex == len(mv.values) {
//				mv.times--
//			}
//
//			mv.currIndex = mv.currIndex % len(mv.values)
//
//			return []*core.Msg{mes}, true, nil
//		}
//	}
//
//	return []*core.Msg{mf.NewDoneMessage()}, true, nil
//}

func (mv *MapValue) HasLocalState() bool {
	return false
}

func (mv *MapValue) SetName(name string) {
	mv.name = name
}

func (mv *MapValue) Name() string {
	return mv.name
}
