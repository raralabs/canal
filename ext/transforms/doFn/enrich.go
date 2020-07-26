package doFn

import (
	"github.com/Knetic/govaluate"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/utils/cast"
)

func EnrichFunction(field string, expr *govaluate.EvaluableExpression, done func(m message.Msg) bool) pipeline.Executor {

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()
		pContent := m.PrevContent()

		if !done(m) {
			// Enrich here

			if content != nil {
				values := content.Values()
				val, err := expr.Evaluate(values)
				if err != nil {
					return false
				}
				v, vt := cast.ValType(val)
				content.Add(field, message.NewFieldValue(v, vt))
			}

			if pContent != nil {
				pValues := pContent.Values()
				pVal, _ := expr.Evaluate(pValues)

				v, vt := cast.ValType(pVal)
				pContent.Add(field, message.NewFieldValue(v, vt))
			}

			proc.Result(m, content, pContent)
			return false
		}

		proc.Result(m, content, pContent)
		proc.Done()
		return false
	})
}

