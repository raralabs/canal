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
	field2 func() string
}

func NewCorrelation(tmpl agg.IAggFuncTemplate, field2 func() string) *Correlation {
	return &Correlation{
		tmpl:   tmpl,
		cor:    stream_math.NewCorrelation(),
		field2: field2,
	}
}

func (c *Correlation) Remove(prevContent *message.OrderedContent) {}

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

		switch val1.ValueType() {
		case message.INT, message.FLOAT:
			x1, _ := cast.TryFloat(val1.Val)
			y1, _ := cast.TryFloat(val2.Val)

			if prevContent != nil {
				vl1, ok1 := prevContent.Get(c.tmpl.Field())
				vl2, ok2 := prevContent.Get(c.field2())

				if ok1 && ok2 {
					x2, _ := cast.TryFloat(vl1.Val)
					y2, _ := cast.TryFloat(vl2.Val)

					c.cor.Replace(x1, y1, x2, y2)
				}

			}

			c.cor.Add(x1, y1)
		}
	}
}

func (c *Correlation) Result() *message.MsgFieldValue {
	res, err := c.cor.Result()
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}
	return message.NewFieldValue(res, message.FLOAT)
}

func (c *Correlation) Name() string {
	return c.tmpl.Name()
}

func (c *Correlation) Reset() {
	c.cor.Reset()
}
