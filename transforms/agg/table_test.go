package agg

import (
	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

type count struct {
	name string
}

func (c *count) Name() string {
	return c.name
}

func (c *count) SetName(name string) {
	c.name = name
}

func (c *count) Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue {
	return message.NewFieldValue(currentValue.Value().(int)+1, message.INT)
}

func (c *count) InitValue() *message.MsgFieldValue {
	return message.NewFieldValue(int(0), message.INT)
}

func (c *count) InitMsgValue(msg *message.MsgContent) *message.MsgFieldValue {
	return message.NewFieldValue(int(1), message.INT)
}

func (c *count) Reset() {
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

func preprocess(m map[string]interface{}) *message.MsgContent {

	mVal := make(message.MsgContent)

	for k, v := range m {
		val, valType := getValType(v)
		mVal.AddMessageValue(k, message.NewFieldValue(val, valType))
	}

	return &mVal
}

func TestTable(t *testing.T) {

	agg1 := &count{name: "Count1"}
	agg2 := &count{name: "Count2"}

	aggs := []Aggregator{agg1, agg2}

	tbl := NewTable(aggs, "name")
	tbl1 := NewTable(aggs)

	value := map[string]interface{}{

		"name":  "Nepal",
		"value": 1,
		"greet": "Hello",
	}

	msg := preprocess(value)

	value1 := map[string]interface{}{

		"value": 1,
		"greet": "Hello",
	}

	msg1 := preprocess(value1)

	for i := 0; i < 3; i++ {
		tbl.Insert(msg)
		tbl.Insert(msg1)

		tbl1.Insert(msg)

		for _, v := range tbl.Messages() {
			assert.Equal(t, int(i+1), v.Values()["Count1"], "")
			assert.Equal(t, int(i+1), v.Values()["Count2"], "")
		}

		for _, v := range tbl1.Messages() {
			assert.Equal(t, int(i+1), v.Values()["Count1"], "")
			assert.Equal(t, int(i+1), v.Values()["Count2"], "")
		}
	}

	_ = agg1
	_ = agg2
}
