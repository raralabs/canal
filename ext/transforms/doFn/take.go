package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/utils/dstr"
)

var descMap = map[string]func(map[string]*dstr.RoundRobin, uint64, []string, *message.OrderedContent){
	"head": func(table map[string]*dstr.RoundRobin, maxRows uint64, cols []string, content *message.OrderedContent) {
		if maxRows > 0 {
			insertMessage(table, cols, content)
		}
	},
	"tail": func(table map[string]*dstr.RoundRobin, maxRows uint64, cols []string, content *message.OrderedContent) {
		insertMessage(table, cols, content)
	},
}

func insertMessage(table map[string]*dstr.RoundRobin, cols []string, content *message.OrderedContent) {
	for _, key := range cols {
		if val, ok := content.Get(key); ok {
			table[key].Put(val)
		} else {
			table[key].Put(message.NewFieldValue(nil, message.NONE))
		}
	}
}

func Take(desc string, num uint64, done func(m message.Msg) bool) pipeline.Executor {
	// Simple Table for Storing required messages
	table := make(map[string]*dstr.RoundRobin)
	var cols []string
	first := true

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		mContent := m.Content()
		if done(m) {
			if len(cols) > 0 {
				depth := table[cols[0]].Len()
				for i := uint64(0); i < depth; i++ {
					content := message.NewOrderedContent()
					for k, v := range table {
						val, _ := v.Get(i)
						content.Add(k, val.(*message.MsgFieldValue))
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
				table[k] = dstr.NewRoundRobin(num)
			}
		}

		descMap[desc](table, num, cols, mContent)
		if num > 0 {
			num--
		}

		return false
	})
}
