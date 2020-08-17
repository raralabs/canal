package doFn

import (
	"github.com/raralabs/canal/core/message"
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func SelectFunction(fields []string, done func(m message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := m.Content()
		if !done(m) {
			content := content2.New()
			for _, fld := range fields {
				if v, ok := mContent.Get(fld); ok {
					content.Add(fld, v)
				} else {
					content.Add(fld, content2.NewFieldValue(nil, content2.NONE))
				}
			}

			proc.Result(m, content, m.PrevContent())
		} else {
			proc.Result(m, mContent, m.PrevContent())
			proc.Done()
		}

		return false
	})
}
