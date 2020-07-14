package doFn

import (
	"time"

	"github.com/raralabs/canal/core/transforms/do"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

func DelayFunction(delay time.Duration) pipeline.Executor {

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		time.Sleep(delay)
		proc.Result(m, m.Content(), nil)
		return true
	}

	return do.NewOperator(df)
}
