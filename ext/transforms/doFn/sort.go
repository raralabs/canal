package doFn

import (
	"github.com/raralabs/canal/core/message"
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/ext/transforms/doFn/sort"
)

func SortFunction(field string, done func(m message.Msg) bool) pipeline.Executor {

	sorter := sort.NewInsertion(field)

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := m.Content()
		if done(m) {
			cols := sorter.Columns()
			for output := range sorter.Iterate() {
				content := content2.New()
				for i, c := range cols {
					content = content.Add(c, output[i])
				}
				proc.Result(m, content, nil)
			}
			proc.Result(m, mContent, nil)
			proc.Done()
			return false
		}

		sorter.Add(mContent)
		return false
	})
}
