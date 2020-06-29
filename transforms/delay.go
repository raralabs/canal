package transforms

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/transforms/base_transforms"
	"time"
)

func DelayFunction(delay time.Duration) pipeline.Executor {

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		time.Sleep(delay)
		proc.Result(m, m.Content())
		return true
	}

	return base_transforms.NewDoOperator(df)
}
