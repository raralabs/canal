package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Covariance struct {
	tmpl agg.IAggFuncTemplate

	cov    *stream_math.Covariance
	field2 func() string
}

func NewCovariance(tmpl agg.IAggFuncTemplate, field2 func() string) *Covariance {
	return &Covariance{
		tmpl:   tmpl,
		cov:    stream_math.NewCovariance(),
		field2: field2,
	}
}

func (c *Covariance) Remove(prevContent *message.OrderedContent) {}

func (c *Covariance) Add(content, prevContent *message.OrderedContent) {
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

					c.cov.Replace(x1, y1, x2, y2)
				}

			}

			c.cov.Add(x1, y1)
		}
	}
}

func (c *Covariance) Result() *message.MsgFieldValue {
	res, _ := c.cov.Result()
	return message.NewFieldValue(res, message.FLOAT)
}

func (c *Covariance) Name() string {
	return c.tmpl.Name()
}

func (c *Covariance) Reset() {
	c.cov.Reset()
}
