package pick

import (
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/utils/dstr"
	"github.com/raralabs/canal/utils/extract"
	"log"
)

type lastPick struct {
	maxRows uint64
	table   map[string]*dstr.RoundRobin
	first   bool
	cols    []string
}

func NewLastPick(maxRows uint64) *lastPick {
	return &lastPick{
		maxRows: maxRows,
		table:   make(map[string]*dstr.RoundRobin),
		first:   true,
	}
}

func (lt *lastPick) Pick(content content.IContent) {
	if lt.first {
		lt.first = false
		lt.cols = extract.Columns(content)
		for _, col := range lt.cols {
			lt.table[col] = dstr.NewRoundRobin(lt.maxRows)
		}
	}

	insertMessage(func(key string, val interface{}) {
		lt.table[key].Put(val)
	}, lt.cols, content)
}

func (lt *lastPick) Messages() []content.IContent {
	var contents []content.IContent

	if len(lt.cols) > 0 {
		depth := lt.table[lt.cols[0]].Len()
		for i := uint64(0); i < depth; i++ {
			cntnt := content.New()
			for _, col := range lt.cols {
				val, err := lt.table[col].Get(i)
				if err != nil {
					log.Printf("[ERROR] %v", err)
					return nil
				}
				cntnt.Add(col, val.(content.MsgFieldValue))
			}
			contents = append(contents, cntnt)
		}
	}

	return contents
}
