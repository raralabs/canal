package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type PValue struct {
	tmpl agg.IAggFuncTemplate

	pv    *stream_math.PValue
	pval  *message.MsgFieldValue
	field2 func() string
}

func NewPValue(tmpl agg.IAggFuncTemplate, field2 func() string) *PValue {
	return &PValue{
		tmpl:   tmpl,
		pv:    stream_math.NewPValue(),
		pval:  message.NewFieldValue(nil, message.NONE),
		field2: field2,
	}
}

func (c *PValue) Add(content, prevContent *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val1, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}
		val2, ok := content.Get(c.field2())
		if !ok {
			return
		}

		if c.pval.Value() == nil {
			c.pval.ValType = message.FLOAT
		}

		switch val1.ValueType() {
		case message.INT, message.FLOAT:
			v1, _ := cast.TryFloat(val1.Val)
			v2, _ := cast.TryFloat(val2.Val)

			c.pv.Add(v1, v2)
		}
	}
}

func (c *PValue) Result() *message.MsgFieldValue {
	res, err := c.pv.Result()
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}
	return message.NewFieldValue(res, c.pval.ValueType())
}

func (c *PValue) Name() string {
	return c.tmpl.Name()
}

func (c *PValue) Reset() {
	c.pv.Reset()
	c.pval.ValType = message.NONE
}
