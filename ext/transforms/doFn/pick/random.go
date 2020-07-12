package pick

import (
	"github.com/raralabs/canal/utils/extract"
	"log"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/maths"
)

type randomPick struct {
	count   uint64
	maxRows uint64
	table   map[string][]interface{}
	first   bool
	cols    []string
}

func NewRandomPick(maxRows uint64) *randomPick {
	return &randomPick{
		count:   uint64(0),
		maxRows: maxRows,
		table:   make(map[string][]interface{}),
		first:   true,
	}
}

func (rt *randomPick) Pick(content *message.OrderedContent) {
	if rt.first {
		rt.first = false
		rt.cols = extract.Columns(content)
	}
	if rt.count < rt.maxRows {
		insertMessage(func(key string, val interface{}) {
			rt.table[key] = append(rt.table[key], val)
		}, rt.cols, content)
	} else {
		if len(rt.cols) > 0 {
			depth := len(rt.table[rt.cols[0]])
			if depth != int(rt.maxRows) {
				log.Panic("Depth should be equal to max rows for random pick.")
			}

			if index, ok := maths.ReservoirSample(rt.maxRows, rt.count); ok {
				// Replace the item at index with current item
				for _, key := range rt.cols {
					if val, ok := content.Get(key); ok {
						rt.table[key][index] = val
					} else {
						rt.table[key][index] = message.NewFieldValue(nil, message.NONE)
					}
				}
			}
		}
	}
	rt.count++
}

func (rt *randomPick) Messages() []*message.OrderedContent {
	var contents []*message.OrderedContent

	if len(rt.cols) > 0 {
		depth := len(rt.table[rt.cols[0]])
		for i := 0; i < depth; i++ {
			content := message.NewOrderedContent()
			for _, col := range rt.cols {
				content.Add(col, rt.table[col][i].(*message.MsgFieldValue))
			}
			contents = append(contents, content)
		}
	}

	return contents
}
