package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func FilterFunction(filter func(map[string]interface{}) (bool, error), doneFunc func(msg message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		sent := false
		if m.Content() != nil {
			match, err := filter(m.Content().Values())

			if err == nil {
				if doneFunc(m) {
					contents := content.Builder(m.Content())
					pContent := content.Builder(m.PrevContent())
					proc.Result(m, contents, pContent)
					proc.Done()
					return false
				}

				if match {
					contents := content.Builder(m.Content())
					pContent := content.Builder(m.PrevContent())

					sent = true
					proc.Result(m, contents, pContent)
				}
			}
		}

		if !sent {

			if m.PrevContent() != nil {
				if ok, _ := filter(m.PrevContent().Values()); ok {
					pContent := content.Builder(m.PrevContent())
					proc.Result(m, nil, pContent)
				}
			}
		}

		return false
	})
}
