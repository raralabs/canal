package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/ext/transforms/doFn/pick"
)

func PickFunction(desc string, num uint64, done func(m message.Msg) bool) pipeline.Executor {

	var picker pick.IPick
	switch desc {
	case "first":
		picker = pick.NewFirstPick(num)
	case "random":
		picker = pick.NewRandomPick(num)
	case "last":
		picker = pick.NewLastPick(num)
	}

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := m.Content()
		if done(m) {
			for _, output := range picker.Messages() {
				proc.Result(m, output)
			}
			proc.Result(m, mContent)
			proc.Done()
			return false
		}

		picker.Pick(mContent)

		return false
	})
}
