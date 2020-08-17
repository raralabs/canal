package pick

import (
	"github.com/raralabs/canal/core/message/content"
)

type IPick interface {
	// Pick picks the content if it satisfies certain condition.
	Pick(content content.IContent)

	// Messages returns all the messages picked by the picker.
	Messages() []content.IContent
}
