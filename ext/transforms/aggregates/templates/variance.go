package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewVariance(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Var"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return functions.NewVariance(ag) }

	return ag
}
