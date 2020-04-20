package poll

import (
	"reflect"
	"testing"

	"github.com/raralabs/tengo/assert"
)

func TestFilterEvent(t *testing.T) {

	var filterEv *FilterEvent

	t.Run("Testing FilterEvent Creation", func(t *testing.T) {
		assert.Nil(t, filterEv, "FilterEvent entity not yet created")
		filterEv = NewFilterEvent(func(m map[string]interface{}) bool {
			if m == nil {
				return false
			}

			return m["value"] == m["val"]
		})
		assert.NotNil(t, filterEv, "FilterEvent entity has been created")
	})

	t.Run("Testing Filter ValueType", func(t *testing.T) {
		if !reflect.DeepEqual(FILTER, filterEv.Type()) {
			t.Errorf("ValueType should be %v, got = %v", FILTER, filterEv.Type())
		}
	})

	m := map[string]interface{}{
		"value": 10,
		"val":   10,
	}

	t.Run("Testing Triggered", func(t *testing.T) {
		assert.False(t, filterEv.Triggered(m), "Filter not yet started")
		filterEv.Start()
		assert.True(t, filterEv.Triggered(m), "Filter has been started")
		filterEv.Stop()
		assert.False(t, filterEv.Triggered(m), "Filter has been stopped")
	})

}
