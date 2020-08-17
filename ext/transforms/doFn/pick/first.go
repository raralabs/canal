package pick

import (
	"github.com/raralabs/canal/core/message/content"
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

func (ft *firstPick) Pick(cntnt content.IContent) {
	if ft.first {
		ft.first = false
		ft.cols = extract.Columns(cntnt)
	}
	if ft.count < ft.maxRows {
		insertMessage(func(key string, val interface{}) {
			ft.table[key] = append(ft.table[key], val)
		}, ft.cols, cntnt)
	}
	ft.count++
}

func (ft *firstPick) Messages() []content.IContent {
	var contents []content.IContent

	if len(ft.cols) > 0 {
		depth := len(ft.table[ft.cols[0]])
		for i := 0; i < depth; i++ {
			cntnt := content.New()
			for _, col := range ft.cols {
				cntnt.Add(col, ft.table[col][i].(*content.MsgFieldValue))
			}
			contents = append(contents, cntnt)
		}
	}

	return contents
}
