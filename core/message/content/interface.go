package content

type IContent interface {
	// Copy creates a new copy and returns it.
	Copy() IContent

	// Get returns the value associated with the key and a flag to denote if
	// the value exists.
	Get(key string) (MsgFieldValue, bool)

	// Add adds a key-value pair to the IContent.
	Add(key string, value MsgFieldValue)

	// Keys returns the keys of the IContent.
	Keys() []string

	// Len() returns the total number of elements in IContent.
	Len() int

	// Values returns a map with just keys and values in the message, without type
	// information in order.
	Values() map[string]interface{}

	// Types returns a map with just keys and values types in the message, without
	// actual in order.
	Types() map[string]FieldValueType

	// String returns string representation of the content
	String() string
}
