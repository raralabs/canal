package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func SelectFunction(fields []string, done func(m message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := m.Content()
		if !done(m) {
			content := message.NewOrderedContent()
			for _, fld := range fields {
				if v, ok := mContent.Get(fld); ok {
					content.Add(fld, v)
				} else {
					content.Add(fld, message.NewFieldValue(nil, message.NONE))
				}
			}

			proc.Result(m, content, nil)
		} else {
			proc.Result(m, mContent, nil)
			proc.Done()
		}

		return false
	})
}
