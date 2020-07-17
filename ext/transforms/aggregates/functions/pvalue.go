package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type PValue struct {
	tmpl agg.IAggFuncTemplate

	pv     *stream_math.PValue
	field2 func() string
}

func NewPValue(tmpl agg.IAggFuncTemplate, field2 func() string) *PValue {
	return &PValue{
		tmpl:   tmpl,
		pv:     stream_math.NewPValue(),
		field2: field2,
	}
}

func (c *PValue) Remove(prevContent *message.OrderedContent) {
	if prevContent != nil {
		vl1, ok1 := prevContent.Get(c.tmpl.Field())
		vl2, ok2 := prevContent.Get(c.field2())

		if ok1 && ok2 {
			x2, _ := cast.TryFloat(vl1.Val)
			y2, _ := cast.TryFloat(vl2.Val)

			c.pv.Remove(x2, y2)
		}
	}
}

func (c *PValue) Add(content *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val1, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}
		val2, ok := content.Get(c.field2())
		if !ok {
			return
		}

		switch val1.ValueType() {
		case message.INT, message.FLOAT:
			x1, _ := cast.TryFloat(val1.Val)
			y1, _ := cast.TryFloat(val2.Val)

			c.pv.Add(x1, y1)
		}
	}
}

func (c *PValue) Result() *message.MsgFieldValue {
	res, err := c.pv.Result()
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}
	return message.NewFieldValue(res, message.FLOAT)
}

func (c *PValue) Name() string {
	return c.tmpl.Name()
}

func (c *PValue) Reset() {
	c.pv.Reset()
}
