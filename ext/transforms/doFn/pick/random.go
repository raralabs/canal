package pick

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/utils/extract"
	"log"

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

func (rt *randomPick) Pick(cntnt content.IContent) {
	if rt.first {
		rt.first = false
		rt.cols = extract.Columns(cntnt)
	}
	if rt.count < rt.maxRows {
		insertMessage(func(key string, val interface{}) {
			rt.table[key] = append(rt.table[key], val)
		}, rt.cols, cntnt)
	} else {
		if len(rt.cols) > 0 {
			depth := len(rt.table[rt.cols[0]])
			if depth != int(rt.maxRows) {
				log.Panic("Depth should be equal to max rows for random pick.")
			}

			if index, ok := maths.ReservoirSample(rt.maxRows, rt.count); ok {
				// Replace the item at index with current item
				for _, key := range rt.cols {
					if val, ok := cntnt.Get(key); ok {
						rt.table[key][index] = val
					} else {
						rt.table[key][index] = content.NewFieldValue(nil, content.NONE)
					}
				}
			}
		}
	}
	rt.count++
}

func (rt *randomPick) Messages() []content.IContent {
	var contents []content.IContent

	if len(rt.cols) > 0 {
		depth := len(rt.table[rt.cols[0]])
		for i := 0; i < depth; i++ {
			cntnt := content.New()
			for _, col := range rt.cols {
				cntnt.Add(col, rt.table[col][i].(content.MsgFieldValue))
			}
			contents = append(contents, cntnt)
		}
	}

	return contents
}
