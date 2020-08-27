package doFn

import (
	"github.com/Knetic/govaluate"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/utils/extract"
)

func EnrichFunction(field string, expr *govaluate.EvaluableExpression, done func(m message.Msg) bool) pipeline.Executor {

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		var contents, pContent content.IContent
		if m.Content() != nil {
			contents = m.Content().Copy()
		}
		if m.PrevContent() != nil {
			pContent = m.PrevContent().Copy()
		}

		if !done(m) {
			// Enrich here

			if contents != nil {
				values := contents.Values()
				val, err := expr.Evaluate(values)
				if err != nil {
					return false
				}
				v, vt := extract.ValType(val)
				contents = contents.Add(field, content.NewFieldValue(v, vt))
			}

			if pContent != nil {
				pValues := pContent.Values()
				pVal, _ := expr.Evaluate(pValues)

				v, vt := extract.ValType(pVal)
				pContent = pContent.Add(field, content.NewFieldValue(v, vt))
			}

			proc.Result(m, contents, pContent)
			return false
		}

		proc.Result(m, contents, pContent)
		proc.Done()
		return false
	})
}
