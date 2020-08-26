package aggregates

import (
	"errors"
	"github.com/raralabs/canal/core/message/content"

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
	valType content.FieldValueType
	first   bool
}

func newMaxFunc(tmpl agg.IAggFuncTemplate) *max {

	mx := &max{
		tmpl:    tmpl,
		valType: content.NONE,
		first:   true,
		fqCnt:   stream_math.NewFreqCounter(),
	}

	return mx
}

func (c *max) Remove(prevContent content.IContent) {
	// Remove the previous fieldVal
	if prevContent != nil {
		if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
			c.fqCnt.Remove(prevVal.Value())
		}
	}
}

func (c *max) Add(cntnt content.IContent) {

	if c.tmpl.Filter(cntnt.Values()) {

		val, ok := cntnt.Get(c.tmpl.Field())
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

func (c *max) Result() content.MsgFieldValue {

	mx, err := c.calculate(c.fqCnt.Values())
	if err != nil {
		return content.NewFieldValue(nil, content.NONE)
	}

	return content.NewFieldValue(mx, content.FLOAT)
}

func (c *max) Name() string {
	return c.tmpl.Name()
}

func (c *max) Reset() {
	c.first = true
	c.valType = content.NONE
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

func maxIface(fieldType content.FieldValueType, a, b interface{}) (interface{}, error) {
	switch fieldType {
	case content.INT, content.FLOAT:
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
