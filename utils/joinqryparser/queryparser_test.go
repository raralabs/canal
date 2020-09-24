package joinqryparser

import (
	"github.com/raralabs/canal/ext/transforms/joinUsingHshMap"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewQueryParser(t *testing.T) {
	query := "SELECT *        FROM table1   c  INNERJOIN  table2 on t1=t2 "
	t.Run("querypaser", func(t *testing.T) {
		queryParser := NewQueryParser(query)
		assert.Equal(t,"SELECT * FROM table1 c INNERJOIN table2 on t1=t2 ",queryParser.query,"the initialization of " +
			"query parser should clean the query by removing extra whitespaces")

		t.Run("preparequery", func(t *testing.T) {
			queryParser.PrepareQuery()
			assert.Equal(t,joinUsingHshMap.INNER,queryParser.JoinType,"Join type mismatch")
			assert.Equal(t,[]string{"*"},queryParser.Select,"Select fields mismatch")
			assert.Equal(t,tableValues{Name: "table1",alias: "c"},queryParser.FirstTable,"first table attributes doesn't match")
			assert.Equal(t,tableValues{Name: "table2"},queryParser.SecondTable,"Second table attributes doesn't match")
			assert.Equal(t,[]string{"t1"},queryParser.Condition.Fields1,"Fields1 didn't match")
			assert.Equal(t,[]string{"t2"},queryParser.Condition.Fields2,"Fields2 didn't match")
			assert.Equal(t,"=",queryParser.Condition.Operator,"Operator didn't match")
		})

	})
}
