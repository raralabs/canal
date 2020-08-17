package pick

import (
	"github.com/raralabs/canal/core/message/content"
)

func insertMessage(appendFunc func(string, interface{}), cols []string, cntnt content.IContent) {
	for _, key := range cols {
		if val, ok := cntnt.Get(key); ok {
			appendFunc(key, val)
		} else {
			appendFunc(key, content.NewFieldValue(nil, content.NONE))
		}
	}
}
