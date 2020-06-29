package poll

import (
	"testing"
)

func TestCompositeEvent(t *testing.T) {

	// var compEvAnd *CompositeEvent
	// var compEvOr *CompositeEvent

	// t.Run("Testing CompositeEvent Creation", func(t *testing.T) {
	// 	assert.Nil(t, compEvAnd, "CompositeEvent entity not yet created")
	// 	filtEv := NewFilterEvent(func(m map[string]interface{}) bool {
	// 		if m == nil {
	// 			return false
	// 		}

	// 		return m["value"] == m["val"]
	// 	})

	// 	timerEv := NewTimerEvent(100 * time.Millisecond)

	// 	compEvAnd = NewCompositeEvent("and", filtEv, timerEv)
	// 	compEvOr = NewCompositeEvent("or", filtEv, timerEv)

	// 	assert.NotNil(t, compEvAnd, "CompositeEvent entity has been created")
	// 	assert.NotNil(t, compEvOr, "CompositeEvent entity has been created")
	// })

	// t.Run("Testing Composite Event ValueType", func(t *testing.T) {
	// 	if !reflect.DeepEqual(COMPOSITE, compEvAnd.Type()) {
	// 		t.Errorf("ValueType should be %v, got = %v", COMPOSITE, compEvAnd.Type())
	// 	}
	// })

	// m := map[string]interface{}{
	// 	"value": 10,
	// 	"val":   10,
	// }

	// t.Run("Testing Triggered", func(t *testing.T) {
	// 	assert.False(t, compEvAnd.Triggered(m), "Composite Event not yet started")
	// 	assert.False(t, compEvOr.Triggered(m), "Composite Event not yet started")
	// 	compEvAnd.Start()
	// 	compEvOr.Start()

	// 	assert.False(t, compEvAnd.Triggered(m), "Composite Event has been started but not triggered")
	// 	assert.True(t, compEvOr.Triggered(m), "Composite Event has been started")

	// 	compEvAnd.Reset()
	// 	compEvOr.Reset()

	// 	time.Sleep(110 * time.Millisecond)
	// 	assert.True(t, compEvAnd.Triggered(m), "Composite Event has been started")
	// 	assert.True(t, compEvOr.Triggered(m), "Composite Event has been started")

	// 	compEvAnd.Stop()
	// 	compEvOr.Stop()
	// 	assert.False(t, compEvAnd.Triggered(m), "Composite Event has been stopped")
	// 	assert.False(t, compEvOr.Triggered(m), "Composite Event has been stopped")
	// })

	// t.Run("Testing Extreme Conditions", func(t *testing.T) {

	// 	dummy := NewCompositeEvent("")

	// 	dummy.Start()
	// 	assert.False(t, dummy.Triggered(m), "Composite Event has been started but not triggered")

	// 	dummy.Stop()
	// 	assert.False(t, dummy.Triggered(m), "Composite Event has been stopped")

	// 	dummy.AddEvents(NewTimerEvent(10 * time.Millisecond))

	// 	dummy.Start()
	// 	assert.False(t, dummy.Triggered(m), "Composite Event has been started but not triggered")
	// 	dummy.Reset()
	// 	time.Sleep(15 * time.Millisecond)
	// 	assert.True(t, dummy.Triggered(m), "Composite Event has been started")
	// 	dummy.Stop()
	// })

}
