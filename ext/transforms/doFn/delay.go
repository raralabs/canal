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

		var contents, pContent content.IContent
		if m.Content() != nil {
			contents = m.Content().Copy()
		}
		if m.PrevContent() != nil {
			pContent = m.PrevContent().Copy()
		}

		time.Sleep(delay)
		proc.Result(m, contents, pContent)
		return true
	}

	return do.NewOperator(df)
}
