package newJoin


// Represents the type of join Inner, LeftOuter
type JoinType uint8

// These are the currently supported types
const (
	INNER     	JoinType = iota + 1 // For inner join
	LEFTOUTER
)

//interface to implement for different types of join
type join interface {
	//returns the type of join
	Type() JoinType

	//joins two stream instances
	Join(inStream1,inStream2,outStream *stream) *stream

	//returns the joined streams of message
	JoinedMsg()[]stream

	//condition for the join
	Condition(query string)

	//Gives the left table
	LeftTable()	[]stream

	//gives the right table
	RightTable() []stream
}