package aggregates

import (
	"github.com/raralabs/canal/core/message/content"
	"sync/atomic"

	"github.com/raralabs/canal/core/transforms/agg"
)

func NewCount(alias string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "count"
	}

	ag := NewAggTemplate(alias, "", filter)

	ag.function = func() agg.IAggFunc { return newCountFunc(ag) }

	return ag
}


type count struct {
	count uint64
	tmpl  agg.IAggFuncTemplate
}

func newCountFunc(tmpl agg.IAggFuncTemplate) *count {
	return &count{
		tmpl: tmpl,
	}
}

func (c *count) Remove(prevContent content.IContent) {
	if prevContent != nil && c.count > 0 {
		c.count--
	}
}

func (c *count) Add(cntnt content.IContent) {
	if c.tmpl.Filter(cntnt.Values()) {
		atomic.AddUint64(&c.count, 1)
	}
}

func (c *count) Result() content.MsgFieldValue {
	return content.NewFieldValue(c.count, content.INT)
}

func (c *count) Name() string {
	return c.tmpl.Name()
}

func (c *count) Reset() {
	c.count = 0
}
