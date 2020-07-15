package functions

import (
	"errors"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Max struct {
	tmpl agg.IAggFuncTemplate

	fieldVals map[interface{}]bool
	valType   message.FieldValueType
	first     bool
}

func NewMax(tmpl agg.IAggFuncTemplate) *Max {
	return &Max{
		tmpl:      tmpl,
		valType:   message.NONE,
		fieldVals: make(map[interface{}]bool),
		first:     true,
	}
}

func (c *Max) Add(content, prevContent *message.OrderedContent) {

	if c.tmpl.Filter(content.Values()) {
		// Remove the previous fieldVal
		if prevContent != nil {
			if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
				if _, ok := c.fieldVals[prevVal.Value()]; ok {
					delete(c.fieldVals, prevVal.Value())
				}
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

		c.fieldVals[val.Value()] = true
	}
}

func (c *Max) Result() *message.MsgFieldValue {
	var mx interface{}
	var err error

	for k := range c.fieldVals {
		if mx == nil {
			mx = k
		} else {
			mx, err = maxIface(c.valType, mx, k)
			if err != nil {
				return message.NewFieldValue(nil, message.NONE)
			}
		}
	}

	return message.NewFieldValue(mx, c.valType)
}

func (c *Max) Name() string {
	return c.tmpl.Name()
}

func (c *Max) Reset() {
}

func maxIface(fieldType message.FieldValueType, a, b interface{}) (interface{}, error) {
	switch fieldType {
	case message.INT, message.FLOAT:
		af,_ := cast.TryFloat(a)
		bf,_ := cast.TryFloat(b)

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
