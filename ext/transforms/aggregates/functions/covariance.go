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
	covar  *message.MsgFieldValue
	field2 func() string
}

func NewCovariance(tmpl agg.IAggFuncTemplate, field2 func() string) *Covariance {
	return &Covariance{
		tmpl:   tmpl,
		cov:    stream_math.NewCovariance(),
		covar:  message.NewFieldValue(nil, message.NONE),
		field2: field2,
	}
}

func (c *Covariance) Add(content *message.OrderedContent) {
	if c.tmpl.Filter(content.Values()) {
		val1, ok := content.Get(c.tmpl.Field())
		if !ok {
			return
		}
		val2, ok := content.Get(c.field2())
		if !ok {
			return
		}

		if c.covar.Value() == nil {
			c.covar.ValType = message.FLOAT
		}

		switch val1.ValueType() {
		case message.INT, message.FLOAT:
			v1, _ := cast.TryFloat(val1.Val)
			v2, _ := cast.TryFloat(val2.Val)

			c.cov.Add(v1, v2)
		}
	}
}

func (c *Covariance) Result() *message.MsgFieldValue {
	res, err := c.cov.Result()
	if err != nil {
		return message.NewFieldValue(nil, message.NONE)
	}
	return message.NewFieldValue(res, c.covar.ValueType())
}

func (c *Covariance) Name() string {
	return c.tmpl.Name()
}

func (c *Covariance) Reset() {
	c.cov.Reset()
	c.covar.ValType = message.NONE
}
