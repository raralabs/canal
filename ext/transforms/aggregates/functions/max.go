package functions

import (
	"errors"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Max struct {
	tmpl agg.IAggFuncTemplate

	fqCnt   *stream_math.FreqCounter
	valType message.FieldValueType
	first   bool
}

func NewMax(tmpl agg.IAggFuncTemplate) *Max {

	max := &Max{
		tmpl:    tmpl,
		valType: message.NONE,
		first:   true,
		fqCnt:   stream_math.NewFreqCounter(),
	}

	return max
}

func (c *Max) Remove(prevContent *message.OrderedContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
			c.fqCnt.Remove(prevVal.Value())
		}
	}
}

func (c *Max) Add(content, prevContent *message.OrderedContent) {

	if c.tmpl.Filter(content.Values()) {

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

func (c *Max) Result() *message.MsgFieldValue {

	mx, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(mx, message.FLOAT)
}

func (c *Max) Name() string {
	return c.tmpl.Name()
}

func (c *Max) Reset() {
	c.first = true
	c.valType = message.NONE
	c.fqCnt.Reset()
}

func (c *Max) calculate(m map[interface{}]uint64) (interface{}, error) {
	var mx interface{}
	var err error

	for k, v := range m {
		if v > 0 {
			if mx == nil {
				mx = k
			} else {
				mx, err = maxIface(c.valType, mx, k)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return mx, nil

}

func maxIface(fieldType message.FieldValueType, a, b interface{}) (interface{}, error) {
	switch fieldType {
	case message.INT, message.FLOAT:
		af, _ := cast.TryFloat(a)
		bf, _ := cast.TryFloat(b)

		return maxf(af, bf), nil

	}
	return nil, errors.New("unsupported type")
}

func maxf(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
