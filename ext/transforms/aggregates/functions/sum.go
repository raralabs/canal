package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

type Sum struct {
	tmpl agg.IAggFuncTemplate

	lastSum *message.MsgFieldValue
}

func NewSum(tmpl agg.IAggFuncTemplate) *Sum {
	return &Sum{
		tmpl:    tmpl,
		lastSum: message.NewFieldValue(nil, message.NONE),
	}
}

func (c *Sum) Remove(prevContent *message.OrderedContent) {

	if prevContent != nil {
		if old, ok := prevContent.Get(c.tmpl.Field()); ok {
			oldVal, _ := cast.TryFloat(old.Value())
			v1, _ := cast.TryFloat(c.lastSum.Value())
			c.lastSum.Val = v1 - oldVal
		}
	}
}

func (c *Sum) Add(content, prevContent *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		if c.lastSum.Value() == nil {
			c.lastSum.Val = val.Value()
			c.lastSum.ValType = message.FLOAT
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			v1, _ := cast.TryFloat(c.lastSum.Value())
			v2, _ := cast.TryFloat(val.Value())

			sum := v1 + v2

			c.lastSum.Val = sum
		}
	}
}

func (c *Sum) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.lastSum.Value(), c.lastSum.ValueType())
}

func (c *Sum) Name() string {
	return c.tmpl.Name()
}

func (c *Sum) Reset() {
	c.lastSum.Val = nil
	c.lastSum.ValType = message.NONE
}
