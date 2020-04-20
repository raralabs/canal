package poll

import (
	"sync"
	"time"

	"github.com/raralabs/canal/utils/timer"
)

// TimerEvent is a timer event, that should be polled to see if it has been
// triggered.
type TimerEvent struct {
	tmr *timer.Timer

	muRunning *sync.Mutex
	running   bool

	muTmrFired *sync.Mutex
	tmrFired   bool

	muSessionStart *sync.Mutex
	sessionStart   bool
	tmrVal         bool
}

// NewTimerEvent creates a new timer event on the basis of the provided interval.
func NewTimerEvent(interval time.Duration) *TimerEvent {

	te := &TimerEvent{}
	tmr := timer.NewTimer(interval, te.callback)

	te.tmr = tmr
	te.muRunning = &sync.Mutex{}
	te.running = false

	te.muTmrFired = &sync.Mutex{}
	te.tmrFired = false

	te.muSessionStart = &sync.Mutex{}
	te.sessionStart = false

	return te
}

// callback is called whenever the timer fires.
func (te *TimerEvent) callback() {

	te.muTmrFired.Lock()
	te.tmrFired = true
	te.muTmrFired.Unlock()
}

// start starts the event.
func (te *TimerEvent) Start() {
	te.tmr.StartTimer()

	te.muRunning.Lock()
	te.running = true
	te.muRunning.Unlock()
}

// Stop stops the event.
func (te *TimerEvent) Stop() {
	te.muRunning.Lock()
	te.running = false
	te.muRunning.Unlock()

	te.tmr.StopTimer()
}

// ValueType returns the type of the event.
func (te *TimerEvent) Type() EventType { return TIMER }

// Triggered blocks till the trigger has been produced.
func (te *TimerEvent) Triggered(map[string]interface{}) bool {

	te.muRunning.Lock()
	defer te.muRunning.Unlock()
	if !te.running {
		return false
	}

	te.muTmrFired.Lock()
	te.muSessionStart.Lock()
	if !te.sessionStart {
		te.sessionStart = true
		te.tmrVal = te.tmrFired
		te.tmrFired = false
	}
	te.muSessionStart.Unlock()
	te.muTmrFired.Unlock()

	return te.tmrVal
}

func (te *TimerEvent) Reset() {
	te.muSessionStart.Lock()
	te.sessionStart = false
	te.muSessionStart.Unlock()
}
