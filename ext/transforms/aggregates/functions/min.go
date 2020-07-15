package functions

import (
	"errors"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Min struct {
	tmpl agg.IAggFuncTemplate

	fieldVals map[interface{}]uint64
	valType   message.FieldValueType
	first     bool
}

func NewMin(tmpl agg.IAggFuncTemplate) *Min {
	return &Min{
		tmpl:      tmpl,
		valType:   message.NONE,
		fieldVals: make(map[interface{}]uint64),
		first:     true,
	}
}

func (c *Min) Add(content, prevContent *message.OrderedContent) {

	if c.tmpl.Filter(content.Values()) {
		// Remove the previous fieldVal
		if prevContent != nil {
			if prevVal, ok := prevContent.Get(c.tmpl.Field()); ok {
				k := prevVal.Value()
				if v, ok := c.fieldVals[k]; ok {
					if v > 1 {
						c.fieldVals[k]--
					} else {
						delete(c.fieldVals, prevVal.Value())
					}
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

		k := val.Value()
		if v, ok := c.fieldVals[k]; ok {
			if v == 0 {
				c.fieldVals[k] = uint64(1)
			} else {
				c.fieldVals[k]++
			}
		}
	}
}

func (c *Min) Result() *message.MsgFieldValue {
	var mn interface{}
	var err error

	for k := range c.fieldVals {
		if mn == nil {
			mn = k
		} else {
			mn, err = minIface(c.valType, mn, k)
			if err != nil {
				return message.NewFieldValue(nil, message.NONE)
			}
		}
	}

	return message.NewFieldValue(mn, message.FLOAT)
}

func (c *Min) Name() string {
	return c.tmpl.Name()
}

func (c *Min) Reset() {
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
