package tfm

import (
	"testing"
)

func TestValid(t *testing.T) {
	t.Run("Undefined Variables are valid", func(t *testing.T) {
		stmt := NewStmt(`
len(str("a") + str("b")) > 5
		`)
		val, err := stmt.Run()
		if err != nil {
			t.Error(err)
		}
		println(val.(bool))
	})
}
