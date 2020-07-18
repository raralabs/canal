package aggregates

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
)

func NewSum(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "sum"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return newSumFunc(ag) }

	return ag
}


type sum struct {
	tmpl agg.IAggFuncTemplate

	lastSum *message.MsgFieldValue
}

func newSumFunc(tmpl agg.IAggFuncTemplate) *sum {
	return &sum{
		tmpl:    tmpl,
		lastSum: message.NewFieldValue(nil, message.NONE),
	}
}

func (c *sum) Remove(prevContent *message.OrderedContent) {

	if prevContent != nil {
		if old, ok := prevContent.Get(c.tmpl.Field()); ok {
			oldVal, _ := cast.TryFloat(old.Value())
			v1, _ := cast.TryFloat(c.lastSum.Value())
			c.lastSum.Val = v1 - oldVal
		}
	}
}

func (c *sum) Add(content *message.OrderedContent) {
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

func (c *sum) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.lastSum.Value(), c.lastSum.ValueType())
}

func (c *sum) Name() string {
	return c.tmpl.Name()
}

func (c *sum) Reset() {
	c.lastSum.Val = nil
	c.lastSum.ValType = message.NONE
}
