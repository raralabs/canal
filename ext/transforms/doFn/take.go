package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

func TakeHead(num uint64, done func(m message.Msg) bool) pipeline.Executor {
	// Simple Table for Storing required messages
	table := make(map[string][]*message.MsgFieldValue)
	var cols []string
	first := true

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := m.Content()
		if done(m) {
			if len(cols) > 0 {
				depth := len(table[cols[0]])
				for i := 0; i < depth; i++ {
					content := message.NewOrderedContent()
					for k, v := range table {
						content.Add(k, v[i])
					}
					proc.Result(m, content)
				}
			}
			proc.Result(m, mContent)
			proc.Done()
			return false
		}

		if first {
			first = false
			for e := mContent.First(); e != nil; e = e.Next() {
				k, _ := e.Value.(string)
				cols = append(cols, k)
			}
		}

		if num > 0 {
			for _, key := range cols {
				if val, ok := mContent.Get(key); ok {
					table[key] = append(table[key], val)
				} else {
					table[key] = append(table[key], message.NewFieldValue(nil, message.NONE))
				}
			}
		}
		num--

		return false
	})
}
