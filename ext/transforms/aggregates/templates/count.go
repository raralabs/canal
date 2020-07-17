package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

func NewCount(alias string, filter func(map[string]interface{}) bool) *AggTemplate {
	if alias == "" {
		alias = "Count"
	}

	ag := NewAggTemplate(alias, "", filter)

	ag.function = func() agg.IAggFunc { return functions.NewCount(ag) }

	return ag
}
