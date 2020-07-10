package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Max struct {
	name   string
	filter func(map[string]interface{}) bool
	field  string
}

func NewMax(alias, field string, filter func(map[string]interface{}) bool) *Max {
	if alias == "" {
		alias = "Max"
	}
	return &Max{
		name:   alias,
		filter: filter,
		field:  field,
	}
}

func (c *Max) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Max) Function() agg.IAggFunc {
	return functions.NewMax(c)
}

func (c *Max) Name() string {
	return c.name
}

func (c *Max) Field() string {
	return c.field
}
