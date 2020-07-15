package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Mode struct {
	name   string
	filter func(map[string]interface{}) bool
	field  string
}

func NewMode(alias, field string, filter func(map[string]interface{}) bool) *Mode {
	if alias == "" {
		alias = "Mode"
	}
	return &Mode{
		name:   alias,
		filter: filter,
		field:  field,
	}
}

func (c *Mode) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Mode) Function() agg.IAggFunc {
	return functions.NewMode(c)
}

func (c *Mode) Name() string {
	return c.name
}

func (c *Mode) Field() string {
	return c.field
}
