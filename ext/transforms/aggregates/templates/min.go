package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Min struct {
	name string
	filter func(map[string]interface{}) bool
	field string
}

func NewMin(alias, field string, filter func(map[string]interface{}) bool) *Min {
	return &Min{
		name: alias,
		filter: filter,
		field: field,
	}
}

func (c *Min) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Min) Function() agg.IAggFunc {
	return functions.NewMin(c)
}

func (c *Min) Name() string {
	return c.name
}

func (c *Min) Field() string {
	return c.field
}
