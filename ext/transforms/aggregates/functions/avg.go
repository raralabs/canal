package functions

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/utils/cast"
	stream_math "github.com/raralabs/canal/utils/stream-math"
)

type Avg struct {
	tmpl agg.IAggFuncTemplate

	avg *stream_math.Mean
}

func NewAvg(tmpl agg.IAggFuncTemplate) *Avg {
	return &Avg{
		tmpl: tmpl,
		avg:  stream_math.NewMean(),
	}
}

func (c *Avg) Remove(prevContent *message.OrderedContent) {}

func (c *Avg) Add(content, prevContent *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}

		switch val.ValueType() {
		case message.INT, message.FLOAT:
			v, _ := cast.TryFloat(val.Value())
			if prevContent != nil {
				if old, ok := prevContent.Get(c.tmpl.Field()); ok {
					v1, _ := cast.TryFloat(old.Value())
					c.avg.Replace(v1, v)
					return
				}
			}
			c.avg.Add(v)
		}
	}
}

func (c *Avg) Result() *message.MsgFieldValue {
	res, _ := c.avg.Result()
	return message.NewFieldValue(res, message.FLOAT)
}

func (c *Avg) Name() string {
	return c.tmpl.Name()
}

func (c *Avg) Reset() {
	c.avg.Reset()
}
