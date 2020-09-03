package doFn

import (
	"github.com/raralabs/canal/core/message/content"
	"time"

	"github.com/raralabs/canal/core/transforms/do"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

func DelayFunction(delay time.Duration) pipeline.Executor {

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		contents := content.Builder(m.Content())
		pContent := content.Builder(m.PrevContent())

		time.Sleep(delay)
		proc.Result(m, contents, pContent)
		return true
	}

	return do.NewOperator(df)
}