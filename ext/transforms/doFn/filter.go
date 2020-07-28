package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func FilterFunction(filter func(map[string]interface{}) (bool, error), doneFunc func(msg message.Msg) bool) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		sent := false
		if content := m.Content(); content != nil {
			match, err := filter(content.Values())

			if err == nil {
				if doneFunc(m) {
					proc.Result(m, m.Content(), m.PrevContent())
					proc.Done()
					return false
				}

				if match {
					sent = true
					proc.Result(m, m.Content(), m.PrevContent())
				}
			}
		}

		if !sent {
			if pContent := m.PrevContent(); pContent != nil {
				if ok, _ := filter(pContent.Values()); ok {
					proc.Result(m, nil, m.PrevContent())
				}
			}
		}

		return false
	})
}
