package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func PassFunction() pipeline.Executor {

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		contents := content.Builder(m.Content())
		pContent := content.Builder(m.PrevContent())

		proc.Result(m, contents, pContent)
		return true
	}

	return do.NewOperator(df)
}
