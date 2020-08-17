package table

import (
	"github.com/raralabs/canal/core/message/content"
)

// A Context is stateless. It does not updates it's internal values on the basis
// of the data inserted to the table.
type Context interface {
	// Insertable checks if the given message value is insertable to the
	// table on the basis of it's context
	Insertable(Table, content.IContent) bool

	// Insert inserts a message to the table passed to it, keeping the
	// context.
	Insert(Table, content.IContent)
}
