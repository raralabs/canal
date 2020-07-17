package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewMax(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Max"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return functions.NewMax(ag) }

	return ag
}
