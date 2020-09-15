package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/dstr"
	"github.com/raralabs/canal/utils/timer"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

//func TestMetaData_newMetaData(t *testing.T){
//	t.Run("newMetaData", func(t *testing.T) {
//		metaData := newMetadata()
//	})
//
//}

func TestMetaData_messageRate(t *testing.T){
	t.Run("newMetaData", func(t *testing.T) {
		msgRate:= newMessageRate(10,uint64(4))
		assert.Equal(t,time.Duration(10),msgRate.duration,"duration mismatch")
		assert.Equal(t,uint64(4),msgRate.bufSize,"bufsize different then what was set")
		assert.Equal(t,"sync.Mutex",reflect.TypeOf(msgRate.rateMu).String(),"data type mismatch for rateMu")
	})
}
//Test
// - TimerRunning()
// - data()
// - ping()

func TestMetaData_MessageRate_Attr(t *testing.T){
	metaData := newMetadata()
	t.Run("start Message Rate", func(t *testing.T) {
		assert.Equal(t,false,metaData.msgPerSec.tmr.TimerRunning(),"Timer not started")
		metaData.msgPerSec.tmr.TimerRunning()
		metaData.msgPerSec.start()
		assert.Equal(t,true,metaData.msgPerSec.tmr.TimerRunning(),"Timer started")
		buf := metaData.msgPerSec.data()
		assert.Equal(t,false,metaData.msgPerMin.tmr.TimerRunning(),"Timer not started")
		metaData.msgPerSec.tmr.TimerRunning()
		assert.Equal(t,"[]interface {}",reflect.TypeOf(buf).String())
		assert.Equal(t,uint64(0),metaData.msgPerSec.counter,"unexpect value of the counter")
		metaData.msgPerSec.ping()
		assert.Equal(t,uint64(1),metaData.msgPerSec.counter,"unexpect value of the counter")
	})
}

func TestMetaData_init(t *testing.T){
	msgRate := newMessageRate(10,uint64(4))
	t.Run("init", func(t *testing.T){
		msgRate.rateBuffer = dstr.NewRoundRobin(msgRate.bufSize)
		getData := func() {
			cnt := atomic.SwapUint64(&msgRate.counter, 0)

			msgRate.rateMu.Lock()
			msgRate.rateBuffer.Put(cnt) //???
			msgRate.rateMu.Unlock()
		}
		msgRate.tmr = timer.NewTimer(msgRate.duration, getData)
	})
}

func TestMetaData_ping(t *testing.T){
	msg:= message.Msg{}
	metaData := newMetadata()
	t.Run("lock", func(t *testing.T) {
		assert.Equal(t,uint64(0),metaData.totalRcvMsg)
		metaData.ping(msg)
		assert.Equal(t,uint64(1),metaData.totalRcvMsg)
	})
}

func TestMetaData_lock(t *testing.T){
	metaData := newMetadata()
	t.Run("lock", func(t *testing.T) {
		assert.Equal(t,false,metaData.msgPerSec.tmr.TimerRunning())
		assert.Equal(t,false,metaData.msgPerMin.tmr.TimerRunning())
		assert.Equal(t,false,metaData.msgPerHr.tmr.TimerRunning())
		assert.Equal(t,false,metaData.msgPerDay.tmr.TimerRunning())
		metaData.lock()
		assert.Equal(t,true,metaData.msgPerSec.tmr.TimerRunning())
		assert.Equal(t,true,metaData.msgPerMin.tmr.TimerRunning())
		assert.Equal(t,true,metaData.msgPerHr.tmr.TimerRunning())
		assert.Equal(t,true,metaData.msgPerDay.tmr.TimerRunning())
	})

}

