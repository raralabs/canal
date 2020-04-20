package idiom

import (
	"errors"
	"fmt"

	"github.com/Knetic/govaluate"
	"github.com/n-is/canal/network/builder/utils"
)

var triggerOperatorMap = map[string]string{
	"||": "or",
	"&&": "and",
}

// A secondaryOperatorMap holds the mapping of secondary operators like or, and.
var secondaryOperatorMap = map[string]func(l, r bool) (bool, error){
	"or": func(l, r bool) (bool, error) {
		return l || r, nil
	},
	"and": func(l, r bool) (bool, error) {
		return l && r, nil
	},
}

// newPrimaryFilter returns a filter defined by op.
func newPrimaryFilter(name string, op, val interface{}) Filter {

	operator := ""
	switch op := op.(type) {
	case []uint8:
		operator = string(op)
	case string:
		operator = op
	}

	condition := fmt.Sprintf("%s %s %v", name, operator, val)

	expression, err := govaluate.NewEvaluableExpression(condition)
	if err != nil {
		panic(err)
	}

	filter := func(valMap map[string]interface{}) (bool, error) {
		ret := false

		result, err := expression.Evaluate(valMap)
		if err != nil {
			return false, err
		}

		rbool, ok := result.(bool)
		if !ok {
			return false, errors.New("non boolean result in filter condition")
		} else {
			ret = rbool
		}

		return ret, nil
	}

	return filter
}

// newSecondaryFilter returns a filter defined by rest recursively.
func newSecondaryFilter(first, rest interface{}) Filter {

	filter := func(valMap map[string]interface{}) (bool, error) {

		if valMap == nil {
			return false, nil
		}

		l := first.(Filter)
		outl, _ := l(valMap)

		restSl := utils.ToIfaceSlice(rest)
		for _, v := range restSl {
			restExpr := utils.ToIfaceSlice(v)
			r := restExpr[3].(Filter)
			op := restExpr[1]
			operator := ""
			switch op := op.(type) {
			case []uint8:
				operator = string(op)
			case string:
				operator = op
			}

			outr, _ := r(valMap)
			outl, _ = secondaryOperatorMap[operator](outl, outr)
		}
		return outl, nil
	}

	return filter
}

func newJoinPriFilter(first, op, val interface{}) JoinFilter {
	v1 := first.(JoinValue)
	f1 := fmt.Sprintf("%s%s", v1.Stream, v1.Field)

	var f2 interface{}

	jVals := []JoinValue{v1}

	if v, ok := val.(JoinValue); ok {
		f2 = fmt.Sprintf("%s%s", v.Stream, v.Field)
		jVals = append(jVals, v)
	} else {
		f2 = val
	}

	opr, _ := utils.TryString(op)

	expr := fmt.Sprintf("%s %s %v", f1, opr, f2)

	expression, err := govaluate.NewEvaluableExpression(expr)
	if err != nil {
		panic(err)
	}
	filter := func(valMap map[string]interface{}) (bool, error) {
		ret := false

		result, err := expression.Evaluate(valMap)
		if err != nil {
			return false, err
		}

		rbool, ok := result.(bool)
		if !ok {
			return false, errors.New("non boolean result in filter condition")
		} else {
			ret = rbool
		}

		return ret, nil
	}

	return JoinFilter{Filter: filter, JoinValues: jVals}
}

func newJoinSecFilter(first, rest interface{}) JoinFilter {

	var jVals []JoinValue
	l := first.(JoinFilter)
	jVals = append(jVals, l.JoinValues...)

	restSl := utils.ToIfaceSlice(rest)
	for _, v := range restSl {
		restExpr := utils.ToIfaceSlice(v)
		r := restExpr[3].(JoinFilter)

		for _, v := range r.JoinValues {
			eq := false
			for _, v1 := range jVals {
				if v == v1 {
					eq = true
				}
			}
			if !eq {
				jVals = append(jVals, v)
			}
		}
	}

	filter := func(valMap map[string]interface{}) (bool, error) {

		if valMap == nil {
			return false, nil
		}

		l := first.(JoinFilter)
		outl, _ := l.Filter(valMap)

		restSl := utils.ToIfaceSlice(rest)
		for _, v := range restSl {
			restExpr := utils.ToIfaceSlice(v)
			r := restExpr[3].(JoinFilter)
			op := restExpr[1]
			operator := ""
			switch op := op.(type) {
			case []uint8:
				operator = string(op)
			case string:
				operator = op
			}

			outr, _ := r.Filter(valMap)
			outl, _ = secondaryOperatorMap[operator](outl, outr)
		}
		return outl, nil
	}

	return JoinFilter{Filter: filter, JoinValues: jVals}
}
