package functions

import (
	"errors"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Min struct {
	tmpl agg.IAggFuncTemplate

	fqCnt   *stream_math.FreqCounter
	valType message.FieldValueType
	first   bool
}

func NewMin(tmpl agg.IAggFuncTemplate) *Min {
	return &Min{
		tmpl:    tmpl,
		valType: message.NONE,
		first:   true,
		fqCnt:   stream_math.NewFreqCounter(),
	}
}

func (c *Min) Add(content, prevContent *message.OrderedContent) {

	if c.tmpl.Filter(content.Values()) {
		// Remove the previous fieldVal
		if prevContent != nil {
			if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
				c.fqCnt.Remove(prevVal.Value())
			}
		}

		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.first {
			c.first = false
			c.valType = val.ValueType()
		}

		k := val.Value()
		c.fqCnt.Add(k)
	}
}

func (c *Min) Result() *message.MsgFieldValue {

	mn, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(mn, message.FLOAT)
}

func (c *Min) Name() string {
	return c.tmpl.Name()
}

func (c *Min) Reset() {
	c.first = true
	c.valType = message.NONE
	c.fqCnt.Reset()
}

func (c *Min) calculate(m map[interface{}]uint64) (interface{}, error) {
	var mx interface{}
	var err error

	for k := range m {
		if mx == nil {
			mx = k
		} else {
			mx, err = minIface(c.valType, mx, k)
			if err != nil {
				return nil, err
			}
		}
	}
	return mx, nil

}

func minIface(fieldType message.FieldValueType, a, b interface{}) (interface{}, error) {
	switch fieldType {
	case message.INT, message.FLOAT:
		af, _ := cast.TryFloat(a)
		bf, _ := cast.TryFloat(b)

		return minf(af, bf), nil

	}
	return nil, errors.New("unsupported type")
}

func minf(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
