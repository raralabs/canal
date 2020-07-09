package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Variance struct {
	name string
	filter func(map[string]interface{}) bool
	field string
}

func NewVariance(alias, field string, filter func(map[string]interface{}) bool) *Variance {
	if alias == "" {
		alias = "Variance"
	}
	return &Variance{
		name: alias,
		filter: filter,
		field: field,
	}
}

func (c *Variance) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Variance) Function() agg.IAggFunc {
	return functions.NewVariance(c)
}

func (c *Variance) Name() string {
	return c.name
}

func (c *Variance) Field() string {
	return c.field
}
