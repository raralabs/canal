package newJoin

import (
	"canal/core/message"
	"sync"
	"time"
)

//denotes the row in table and stream for window
type stream struct{
	timeStamp time.Time
	id 		  uint32
	msg       message.Msg
}

//window to bound the input stream in case of window join strategy
type dataWindow struct{
	startTime 		time.Time
	windowSize 		time.Duration
	slidingWindow	uint32
}

//for table join strategy
type temporalTable struct{
	alias		string
	fields		[]string
	tableMu 	*sync.Mutex
	row         []*stream
}

