package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/ext/transforms/doFn/sort"
)

// SortFunction sorts the messages based on certain keys.
// It only supports int and float for now.
func SortFunction(field string, done func(m message.Msg) bool) pipeline.Executor {

	sorter := sort.NewInsertion(field)

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := content.Builder(m.Content())
		if done(m) {
			cols := sorter.Columns()
			for output := range sorter.Iterate() {
				contents := content.New()
				for i, c := range cols {
					contents = contents.Add(c, output[i])
				}
				proc.Result(m, contents, nil)
			}
			proc.Result(m, mContent, nil)
			proc.Done()
			return false
		}

		sorter.Add(mContent)
		return false
	})
}
