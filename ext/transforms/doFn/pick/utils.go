package pick

import "github.com/raralabs/canal/core/message"

func insertMessage(appendFunc func(string, interface{}), cols []string, content *message.OrderedContent) {
	for _, key := range cols {
		if val, ok := content.Get(key); ok {
			appendFunc(key, val)
		} else {
			appendFunc(key, message.NewFieldValue(nil, message.NONE))
		}
	}
}
