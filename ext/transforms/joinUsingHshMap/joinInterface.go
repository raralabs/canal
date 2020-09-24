package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message/content"
)

// Represents the join strategy like temporal table, hash table, windowing
type JoinStrategy uint8

// Represents the type of join Inner, LeftOuter
type JoinType uint8


// These are the current supported strategy for the joins
const (
	HASH  		JoinStrategy = iota+1
	TABLE
	WINDOW
)
// These are the currently supported types of join
const (
	INNER     	JoinType = iota + 1 // For inner join
	LEFTOUTER
)

type join interface{
	run() string
}
//interface to implement for different types of join
type StreamJoin interface{
	//returns the type of join
	Type() JoinType

	//joins two stream instances
	Join(inStream1,inStream2 content.IContent,selectFields []string)content.IContent

	//returns the joined streams of message

	//get stream from 1st route
	ProcessStreamFirst(msg content.IContent,fields []string)

	//get stream from 2nd route
	ProcessStreamSec(msg content.IContent,fields []string)(interface{},bool)
	//
	//prepares query for the join
	Condition(query string)([]string,[]string)


	////gives the right table
	//RightTable() []stream
}




