package functions

import (
	"sync/atomic"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
)

type Count struct {
	count uint64
	tmpl  agg.IAggFuncTemplate
}

func NewCount(tmpl agg.IAggFuncTemplate) *Count {
	return &Count{
		tmpl: tmpl,
	}
}

func (c *Count) Add(content, prevContent *message.OrderedContent) {
	if prevContent != nil {
		if c.tmpl.Filter(content.Values()) {
			atomic.AddUint64(&c.count, 1)
		}
	}
}

func (c *Count) Result() *message.MsgFieldValue {
	return message.NewFieldValue(c.count, message.INT)
}

func (c *Count) Name() string {
	return c.tmpl.Name()
}

func (c *Count) Reset() {
	c.count = 0
}
