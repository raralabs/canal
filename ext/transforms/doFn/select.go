package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

// SelectFunction selects certain fields from the message and only passes them
func SelectFunction(fields []string, done func(m message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {


		if !done(m) {
			oldContents := m.Content()
			pContent := m.PrevContent()

			contents := content.Builder()
			for _, fld := range fields {
				if v, ok := oldContents.Get(fld); ok {
					contents = contents.Add(fld, v)
				} else {
					contents = contents.Add(fld, content.NewFieldValue(nil, content.NONE))
				}
			}
			if pContent != nil {
				pContents := content.Builder()
				for _, fld := range fields {
					if v, ok := pContent.Get(fld); ok {
						pContents.Add(fld, v)
					} else {
						pContents.Add(fld, content.NewFieldValue(nil, content.NONE))
					}
				}
				proc.Result(m, contents, pContents)
			} else {
				proc.Result(m, contents, nil)
			}
		} else {
			oldContents := content.Builder(m.Content())
			pContent := content.Builder(m.PrevContent())
			proc.Result(m, oldContents, pContent)
			proc.Done()
		}

		return false
	})
}
