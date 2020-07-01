package transforms

import (
	"sync/atomic"
	"time"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/transforms/base_transforms"
)

func PassFunction() pipeline.Executor {
	var cnt int32 = 0

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		time.Sleep(250 * time.Millisecond)
		proc.Result(m, m.Content())
		atomic.AddInt32(&cnt, 1)
		if cnt >= 20 {
			//println("PassFunction Closing processor in ", proc.Id())
			proc.Done()
		}

		return true
	}

	return base_transforms.NewDoOperator(df)
}