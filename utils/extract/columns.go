package extract

import (
	"github.com/raralabs/canal/core/message/content"
)

func Columns(content content.IContent) []string {
	return content.Keys()
}
