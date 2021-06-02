package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/agg"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/utils/extract"
)

func BatchAgg(done, reset func(m message.Msg) bool) pipeline.Executor {

	var batch *agg.Aggregator
	first := true

	after := func(m message.Msg, proc pipeline.IProcessorForExecutor, _, _ []content.IContent) {
		if reset != nil {
			if reset(m) {
				entries := batch.Entries()
				for _, e := range entries {
					proc.Result(m, e, nil)
				}
				batch.Reset()
			}
		}
		if done(m) {
			entries := batch.Entries()
			eof := m.Eof()
			m.SetEof(false)
			for _, e := range entries {
				proc.Result(m, e, nil)
			}
			m.SetEof(eof)

			proc.Result(m, m.Content(), nil)
			proc.Done()
		}
	}

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		if first {
			contents := m.Content()
			if contents != nil {
				groups := extract.Columns(contents)

				batch = agg.NewAggregator([]agg.IAggFuncTemplate{}, after, groups...)
				first = false
			}
		}

		if !first {
			_, _, _ = batch.AggFunc(m, &struct{}{})
			after(m, proc, nil, nil)
		}

		return false
	})
}
