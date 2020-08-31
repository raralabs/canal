package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func FilterFunction(filter func(map[string]interface{}) (bool, error), doneFunc func(msg message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		var contents, pContent content.IContent
		if m.Content() != nil {
			contents = m.Content().Copy()
		}
		if m.PrevContent() != nil {
			pContent = m.PrevContent().Copy()
		}

		sent := false
		if contents != nil {
			match, err := filter(contents.Values())

			if err == nil {
				if doneFunc(m) {
					proc.Result(m, contents, pContent)
					proc.Done()
					return false
				}

				if match {
					sent = true
					proc.Result(m, contents, pContent)
				}
			}
		}

		if !sent {
			if pContent != nil {
				if ok, _ := filter(pContent.Values()); ok {
					proc.Result(m, nil, pContent)
				}
			}
		}

		return false
	})
}
