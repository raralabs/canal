package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
)

type AggTemplate struct {
	name     string
	filter   func(map[string]interface{}) bool
	function func() agg.IAggFunc
	field    string
}

func NewAggTemplate(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {

	return &AggTemplate{
		name:   alias,
		filter: filter,
		field:  field,
	}
}

func (c *AggTemplate) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *AggTemplate) Function() agg.IAggFunc {
	return c.function()
}

func (c *AggTemplate) Name() string {
	return c.name
}

func (c *AggTemplate) Field() string {
	return c.field
}
