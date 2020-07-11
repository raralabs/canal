package pick

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/dstr"
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

func (lt *lastPick) Pick(content *message.OrderedContent) {
	if lt.first {
		lt.first = false
		lt.cols = extractCols(content)
		for _, col := range lt.cols {
			lt.table[col] = dstr.NewRoundRobin(lt.maxRows)
		}
	}

	insertMessage(func(key string, val interface{}) {
		lt.table[key].Put(val)
	}, lt.cols, content)
}

func (lt *lastPick) Messages() []*message.OrderedContent {
	var contents []*message.OrderedContent

	if len(lt.cols) > 0 {
		depth := lt.table[lt.cols[0]].Len()
		for i := uint64(0); i < depth; i++ {
			content := message.NewOrderedContent()
			for _, col := range lt.cols {
				val, err := lt.table[col].Get(i)
				if err != nil {
					log.Printf("[ERROR] %v", err)
					return nil
				}
				content.Add(col, val.(*message.MsgFieldValue))
			}
			contents = append(contents, content)
		}
	}

	return contents
}
