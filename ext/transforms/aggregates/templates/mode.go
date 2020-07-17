package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewMode(alias, field string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Mode"
	}

	ag := NewAggTemplate(alias, field, filter)

	ag.function = func() agg.IAggFunc { return functions.NewMode(ag) }

	return ag
}
