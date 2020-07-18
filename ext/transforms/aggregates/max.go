package aggregates

import (
	"errors"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

func NewMax(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "max"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return newMaxFunc(ag) }

	return ag
}


type max struct {
	tmpl agg.IAggFuncTemplate

	fqCnt   *stream_math.FreqCounter
	valType message.FieldValueType
	first   bool
}

func newMaxFunc(tmpl agg.IAggFuncTemplate) *max {

	mx := &max{
		tmpl:    tmpl,
		valType: message.NONE,
		first:   true,
		fqCnt:   stream_math.NewFreqCounter(),
	}

	return mx
}

func (c *max) Remove(prevContent *message.OrderedContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
			c.fqCnt.Remove(prevVal.Value())
		}
	}
}

func (c *max) Add(content *message.OrderedContent) {

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

func (c *max) Result() *message.MsgFieldValue {

	mx, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}

	return message.NewFieldValue(mx, message.FLOAT)
}

func (c *max) Name() string {
	return c.tmpl.Name()
}

func (c *max) Reset() {
	c.first = true
	c.valType = message.NONE
	c.fqCnt.Reset()
}

func (c *max) calculate(m map[interface{}]uint64) (interface{}, error) {
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
