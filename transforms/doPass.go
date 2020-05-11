package transforms

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/transforms/base_transforms"
	"sync/atomic"
	"time"
)

func PassFunction() pipeline.Executor {
	var cnt int32 = 0

	df := func(m message.Msg, proc *pipeline.Processor) bool {
		time.Sleep(250 * time.Millisecond)
		for i := 0; i < 2; i++ {
			proc.Result(m, m.Content())
			atomic.AddInt32(&cnt, 1)
			if cnt >= 20 {
				//println("PassFunction Closing processor in ", proc.Id())
				proc.Close()
			}
		}

		return true
	}

	return base_transforms.NewDoOperator(df)
}
