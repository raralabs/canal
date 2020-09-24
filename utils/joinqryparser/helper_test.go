package joinqryparser

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHelper_tokenize(t *testing.T){
	type testCase struct{
		testName	string
		testString string
		lenOfString uint64

	}
	tests := []testCase{{"split in terms of white space","SELECT * from table 1",5},
		{"split string containing numbers in terms of white space","1 2 3 4 5 6 7 8 9",9},
	{"split string containing single token","string",1},
	{"split interms of comma","hello,how,do,you do",4},
	}
	for _,test := range tests{
		t.Run("tokenization testing", func(t *testing.T) {
			if test.testName != "split interms of comma"{
				tokens := tokenize(test.testString," ")
				assert.Equal(t,test.lenOfString,uint64(len(tokens)),"number of tokens mismatch")
			}else{
				tokens := tokenize(test.testString,",")
				assert.Equal(t,test.lenOfString,uint64(len(tokens)),"number of tokens mismatch")
			}
		})
	}
}

func TestHelper_getJoinCondition(t *testing.T){
	type testCase struct{
		testName 	string
		querySeg	string
		wantCondition1		string
		wantCondition2		string
		wantOperator		string
	}
	tests :=  []testCase{
		{"condtion with equal operator and single fields",
			"field1=field2",
			"field1",
			"field2",
			"=",
		},
		{"condition equal operator with multiple fields",
			"field1,field2,field3 = sfield1,sfield2,sfield3",
			"field1,field2,field3",
			"sfield1,sfield2,sfield3",
			"=",
		},
		{"condition with greater than operator",
			"field1 > field2",
			"field1",
			"field2",
			">",
		},
		{"condtition with less than operator less or equal",
			"field1, field2>=field3 ,field4",
			"field1,field2",
			"field3,field4",
			">=",
		},
	}
	for _,test := range tests{
		t.Run("getJoinCondition", func(t *testing.T) {
			condition1,condition2,operator := getJoinCondition(test.querySeg)
			assert.Equal(t,test.wantCondition1,condition1,"extracted and exact conditions don't match")
			assert.Equal(t,test.wantCondition2,condition2,"extracted and exact conditions don't match")
			assert.Equal(t,test.wantOperator,operator,"extracted and exact operators don't match")
		})

	}
}