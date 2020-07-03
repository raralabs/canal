package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func FilterFunction(filter func(m message.Msg) (bool, bool, error)) pipeline.Executor {
	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		match, done, err := filter(m)

		if err == nil {
			if done {
				proc.Result(m, m.Content())
				proc.Done()
				return false
			}

			if match {
				c := m.Content()
				proc.Result(m, c)
			}
		}

		return false
	})
}
