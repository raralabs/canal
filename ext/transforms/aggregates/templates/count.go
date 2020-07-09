package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Count struct {
	name string
	filter func(map[string]interface{}) bool
}

func NewCount(alias string, filter func(map[string]interface{}) bool) *Count {
	if alias == "" {
		alias = "Count"
	}
	return &Count{
		name: alias,
		filter: filter,
	}
}

func (c *Count) Filter(m map[string]interface{}) bool {
	return c.filter(m)
}

func (c *Count) Function() agg.IAggFunc {
	return functions.NewCount(c)
}

func (c *Count) Name() string {
	return c.name
}

func (c *Count) Field() string {
	return ""
}
