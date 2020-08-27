package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func SelectFunction(fields []string, done func(m message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		var oldContents, pContent content.IContent
		if m.Content() != nil {
			oldContents = m.Content().Copy()
		}
		if m.PrevContent() != nil {
			pContent = m.PrevContent().Copy()
		}

		if !done(m) {
			contents := content.New()
			for _, fld := range fields {
				if v, ok := oldContents.Get(fld); ok {
					contents = contents.Add(fld, v)
				} else {
					contents = contents.Add(fld, content.NewFieldValue(nil, content.NONE))
				}
			}

			proc.Result(m, contents, pContent)
		} else {
			proc.Result(m, oldContents, pContent)
			proc.Done()
		}

		return false
	})
}
