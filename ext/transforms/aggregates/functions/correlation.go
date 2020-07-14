package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Correlation struct {
	tmpl agg.IAggFuncTemplate

	cor    *stream_math.Correlation
	corr  *message.MsgFieldValue
	field2 func() string
}

func NewCorrelation(tmpl agg.IAggFuncTemplate, field2 func() string) *Correlation {
	return &Correlation{
		tmpl:   tmpl,
		cor:    stream_math.NewCorrelation(),
		corr:  message.NewFieldValue(nil, message.NONE),
		field2: field2,
	}
}

func (c *Correlation) Add(content, prevContent *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val1, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}
		val2, ok := content.Get(c.field2())
		if !ok {
			return
		}

		if c.corr.Value() == nil {
			c.corr.ValType = message.FLOAT
		}

		switch val1.ValueType() {
		case message.INT, message.FLOAT:
			v1, _ := cast.TryFloat(val1.Val)
			v2, _ := cast.TryFloat(val2.Val)

			c.cor.Add(v1, v2)
		}
	}
}

func (c *Correlation) Result() *message.MsgFieldValue {
	res, err := c.cor.Result()
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}
	return message.NewFieldValue(res, c.corr.ValueType())
}

func (c *Correlation) Name() string {
	return c.tmpl.Name()
}

func (c *Correlation) Reset() {
	c.cor.Reset()
	c.corr.ValType = message.NONE
}
