package pick

import "github.com/raralabs/canal/core/message"

type IPick interface {

	// Init initializes the picker.
	Init(maxRows uint64)

	// Pick picks the content if it satisfies certain condition.
	Pick(content *message.OrderedContent)

	// Messages returns all the messages picked by the picker.
	Messages() []*message.OrderedContent
}
