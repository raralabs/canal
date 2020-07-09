package agg

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
)

type countTemplate struct {
	name string
}

func (ct *countTemplate) Filter(m map[string]interface{}) bool {
	return true
}

func (ct *countTemplate) Function() IAggFunc {
	return newCount(ct)
}

func (ct *countTemplate) Name() string {
	return ct.name
}

func (ct *countTemplate) Field() string {
	return ""
}


type countFunction struct {
	count uint64
	tmpl IAggFuncTemplate
}

func newCount(tmpl IAggFuncTemplate) *countFunction {
	return &countFunction{
		tmpl: tmpl,
	}
}

func (c *countFunction) Add(value *message.OrderedContent) {
	atomic.AddUint64(&c.count, 1)
}

func (c *countFunction) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.count, message.INT)
}

func (c *countFunction) Name() string {
	return c.tmpl.Name()
}

func (c *countFunction) Reset() {
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

func preprocess(m map[string]interface{}) *message.OrderedContent {

	mVal := message.NewOrderedContent()

	for k, v := range m {
		val, valType := getValType(v)
		mVal.Add(k, message.NewFieldValue(val, valType))
	}

	return mVal
}

func TestTable(t *testing.T) {

	agg1 := &countTemplate{name: "Count1"}
	agg2 := &countTemplate{name: "Count2"}

	aggs := []IAggFuncTemplate{agg1, agg2}

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

		for _, v := range tbl.Entries() {
			assert.Equal(t, uint64(i+1), v.Values()["Count1"], "")
			assert.Equal(t, uint64(i+1), v.Values()["Count2"], "")
			assert.Equal(t, "Nepal", v.Values()["name"], "")
		}

		for _, v := range tbl1.Entries() {
			assert.Equal(t, uint64(i+1), v.Values()["Count1"], "")
			assert.Equal(t, uint64(i+1), v.Values()["Count2"], "")
		}
	}
}
