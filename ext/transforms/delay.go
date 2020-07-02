package transforms

import (
	"time"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/transforms/base_transforms"
)

func DelayFunction(delay time.Duration) pipeline.Executor {

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		time.Sleep(delay)
		proc.Result(m, m.Content())
		return true
	}

	return base_transforms.NewDoOperator(df)
}
