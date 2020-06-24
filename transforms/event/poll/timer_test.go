package poll

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimerEvent(t *testing.T) {

	var timerEv *TimerEvent

	t.Run("Testing TimerEvent Creation", func(t *testing.T) {
		assert.Nil(t, timerEv, "TimerEvent entity not yet created")
		timerEv = NewTimerEvent(100 * time.Millisecond)
		assert.NotNil(t, timerEv, "TimerEvent entity has been created")
	})

	t.Run("Testing Timer ValueType", func(t *testing.T) {
		if !reflect.DeepEqual(TIMER, timerEv.Type()) {
			t.Errorf("ValueType should be %v, got = %v", TIMER, timerEv.Type())
		}
	})

	m := map[string]interface{}{
		"value": 10,
		"val":   10,
	}

	t.Run("Testing Triggered", func(t *testing.T) {
		assert.False(t, timerEv.Triggered(m), "Timer not yet started")
		timerEv.Reset()
		timerEv.Start()
		assert.False(t, timerEv.Triggered(m), "Timer has been started but should not be triggered")
		timerEv.Reset()
		time.Sleep(150 * time.Millisecond)
		assert.True(t, timerEv.Triggered(m), "Timer should have been triggered1")
		assert.True(t, timerEv.Triggered(m), "Timer should have been triggered2")
		assert.True(t, timerEv.Triggered(m), "Timer should have been triggered3")
		timerEv.Reset()
		time.Sleep(100 * time.Millisecond)
		assert.True(t, timerEv.Triggered(m), "Timer has been triggered again")
		timerEv.Stop()
		assert.False(t, timerEv.Triggered(m), "Timer has been stopped")
	})

}
