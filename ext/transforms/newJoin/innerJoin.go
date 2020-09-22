package newJoin

type InnerJoin struct{
	firstStreamPath	 string
	secondStreamPath string
	firstStream		 []stream
	secondStream	 []stream
	joinedStream	 []stream
}


func newInnerJoin(firstStreamPath,secondStreamPath string)*InnerJoin{
	return &InnerJoin{firstStreamPath:firstStreamPath,secondStreamPath: secondStreamPath}

}

