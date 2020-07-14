package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func PassFunction() pipeline.Executor {

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		proc.Result(m, m.Content(), nil)
		return true
	}

	return do.NewOperator(df)
}
