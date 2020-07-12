package templates

import (
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/ext/transforms/aggregates/functions"
)

type PValue struct {
	name   string
	filter func(map[string]interface{}) bool
	field1  string
	field2  string
}

func NewPValue(alias, field1, field2 string, filter func(map[string]interface{}) bool) *PValue {
	if alias == "" {
		alias = "Pvalue"
	}
	return &PValue{
		name:   alias,
		filter: filter,
		field1:  field1,
		field2: field2,
	}
}

func (cv *PValue) Filter(m map[string]interface{}) bool {
	return cv.filter(m)
}

func (cv *PValue) Function() agg.IAggFunc {
	return functions.NewPValue(cv, cv.Field2)
}

func (cv *PValue) Name() string {
	return cv.name
}

func (cv *PValue) Field() string {
	return cv.field1
}

func (cv *PValue) Field2() string {
	return cv.field2
}
