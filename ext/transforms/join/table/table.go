package table

import (
	"github.com/raralabs/canal/core/message/content"
)

// A Table is a data structure that holds the data in tabular format. It supports
// insertion of data in a messageContent format.
type Table interface {
	// Alias gives the name of the table.
	Alias() string

	// Insertable checks if the given message value is insertable to the
	// table on the basis of it's context.
	Insertable(content.IContent) bool

	// Insert inserts the message value to the table.
	Insert(content.IContent)

	// Append appends a row of values to the table.
	Append([]content.MsgFieldValue)

	// Len returns the number of rows in the table.
	Len() int

	// AddFields adds a column to the table. This function works only if the
	// length of the table is 0 i.e. table.Len() == 0
	AddFields(...string)

	// Returns the fields of the table.
	Fields() []string

	// RemoveRow removes a row from the table.
	RemoveRow(int)

	// Iterate through the rows of the table. The values are in the same
	// order as the fields of the table.
	IterRows() <-chan []*content.MsgFieldValue
}
