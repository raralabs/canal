package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Sum struct {
	name   string
	filter func(map[string]interface{}) bool
	field  string
}

func NewSum(alias, field string, filter func(map[string]interface{}) bool) *Sum {
	if alias == "" {
		alias = "Sum"
	}
	return &Sum{
		name:   alias,
		filter: filter,
		field:  field,
	}
}

func (c *Sum) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Sum) Function() agg.IAggFunc {
	return functions.NewSum(c)
}

func (c *Sum) Name() string {
	return c.name
}

func (c *Sum) Field() string {
	return c.field
}
