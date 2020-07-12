package extract

import "github.com/raralabs/canal/core/message"

func Columns(content *message.OrderedContent) []string {
	var cols []string
	for e := content.First(); e != nil; e = e.Next() {
		k, _ := e.Value.(string)
		cols = append(cols, k)
	}

	return cols
}
