package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/utils/extract"
)

func BatchAgg(done func(m message.Msg) bool) pipeline.Executor {

	var batch *agg.Aggregator
	first := true

	after := func(m message.Msg, proc pipeline.IProcessorForExecutor, contents, prevContents []content.IContent) {
		if done(m) {
			entries := batch.Entries()
			for _, e := range entries {
				proc.Result(m, e, nil)
			}

			proc.Result(m, m.Content(), nil)
			proc.Done()
		}
	}

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		if first {
			content := m.Content()
			if content != nil {
				groups := extract.Columns(content)

				batch = agg.NewAggregator([]agg.IAggFuncTemplate{}, after, groups...)
				first = false
			}
		}

		if !first {
			batch.AggFunc(m, &struct{}{})
			after(m, proc, nil, nil)
		}

		return false
	})
}
