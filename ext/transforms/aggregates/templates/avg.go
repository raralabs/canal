package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Avg struct {
	name string
	filter func(map[string]interface{}) bool
	field string
}

func NewAvg(alias, field string, filter func(map[string]interface{}) bool) *Avg {
	if alias == "" {
		alias = "Avg"
	}
	return &Avg{
		name: alias,
		filter: filter,
		field: field,
	}
}

func (c *Avg) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Avg) Function() agg.IAggFunc {
	return functions.NewAvg(c)
}

func (c *Avg) Name() string {
	return c.name
}

func (c *Avg) Field() string {
	return c.field
}
