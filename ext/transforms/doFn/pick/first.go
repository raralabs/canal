package pick

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/extract"
)

type firstPick struct {
	count   uint64
	maxRows uint64
	table   map[string][]interface{}
	first   bool
	cols    []string
}

func NewFirstPick(maxRows uint64) *firstPick {
	return &firstPick{
		count:   uint64(0),
		maxRows: maxRows,
		table:   make(map[string][]interface{}),
		first:   true,
	}
}

func (ft *firstPick) Pick(content *message.OrderedContent) {
	if ft.first {
		ft.first = false
		ft.cols = extract.Columns(content)
	}
	if ft.count < ft.maxRows {
		insertMessage(func(key string, val interface{}) {
			ft.table[key] = append(ft.table[key], val)
		}, ft.cols, content)
	}
	ft.count++
}

func (ft *firstPick) Messages() []*message.OrderedContent {
	var contents []*message.OrderedContent

	if len(ft.cols) > 0 {
		depth := len(ft.table[ft.cols[0]])
		for i := 0; i < depth; i++ {
			content := message.NewOrderedContent()
			for _, col := range ft.cols {
				content.Add(col, ft.table[col][i].(*message.MsgFieldValue))
			}
			contents = append(contents, content)
		}
	}

	return contents
}
