package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type Correlation struct {
	name   string
	filter func(map[string]interface{}) bool
	field1  string
	field2  string
}

func NewCorrelation(alias, field1, field2 string, filter func(map[string]interface{}) bool) *Correlation {
	if alias == "" {
		alias = "Correlation"
	}
	return &Correlation{
		name:   alias,
		filter: filter,
		field1:  field1,
		field2: field2,
	}
}

func (cv *Correlation) Filter(m map[string]interface{}) bool {
	return cv.filter(m)
}

func (cv *Correlation) Function() agg.IAggFunc {
	return functions.NewCorrelation(cv, cv.Field2)
}

func (cv *Correlation) Name() string {
	return cv.name
}

func (cv *Correlation) Field() string {
	return cv.field1
}

func (cv *Correlation) Field2() string {
	return cv.field2
}
