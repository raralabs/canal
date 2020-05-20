package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/utils/dstr"
	"github.com/raralabs/canal/utils/timer"
	"sync"
	"sync/atomic"
	"time"
)

// metadata is a data holder that is used to store information generated during execution.
type metadata struct {
	dataMu *sync.Mutex

	totalRcvMsg  uint64
	lastRcvMid   uint64
	startTime    time.Time
	totalRuntime time.Duration

	// Buckets for message rate
	msgPerSec *messageRate
	msgPerMin *messageRate
	msgPerHr  *messageRate
	msgPerDay *messageRate
}

// newMetadata creates a new metadata and returns it.
func newMetadata() *metadata {
	m := &metadata{
		dataMu:    &sync.Mutex{},

		msgPerSec: newMessageRate(1*time.Second, 30),
		msgPerMin: newMessageRate(1*time.Minute, 15),
		msgPerHr:  newMessageRate(1*time.Hour, 10),
		msgPerDay: newMessageRate(24*time.Hour, 7),
	}

	m.msgPerSec.init()
	m.msgPerMin.init()
	m.msgPerHr.init()
	m.msgPerDay.init()

	return m
}

// lock initializes the metadata
func (m *metadata) lock() {
	m.startTime = time.Now()

	m.msgPerSec.start()
	m.msgPerMin.start()
	m.msgPerHr.start()
	m.msgPerDay.start()
}

func (m *metadata) ping(msg message.Msg) {
	m.totalRcvMsg++
	m.lastRcvMid = msg.Id()
	m.totalRuntime = time.Since(m.startTime)

	m.msgPerSec.ping()
	m.msgPerMin.ping()
	m.msgPerHr.ping()
	m.msgPerDay.ping()
}

func (m *metadata) done() {
	//fmt.Println(m.msgPerSec.data())
	//fmt.Println("Done Dunn Dun ...")
}

// A messageRate holds the message rate over certain duration over certain time in a buffer
type messageRate struct {
	duration   time.Duration
	bufSize    uint64
	rateMu     sync.Mutex
	rateBuffer *dstr.RoundRobin
	tmr        *timer.Timer
	counter    uint64
}

func newMessageRate(d time.Duration, bufSz uint64) *messageRate {
	return &messageRate{
		duration: d,
		bufSize:  bufSz,
		rateMu:   sync.Mutex{},
	}
}

func (mr *messageRate) init() {

	mr.rateBuffer = dstr.NewRoundRobin(mr.bufSize)

	getData := func() {
		cnt := atomic.SwapUint64(&mr.counter, 0)

		mr.rateMu.Lock()
		mr.rateBuffer.Put(cnt)
		mr.rateMu.Unlock()
	}

	mr.tmr = timer.NewTimer(mr.duration, getData)
}

func (mr *messageRate) start() {
	mr.tmr.StartTimer()
}

func (mr *messageRate) ping() {
	atomic.AddUint64(&mr.counter, 1)
}

func (mr *messageRate) data() []interface{} {
	mr.rateMu.Lock()
	defer mr.rateMu.Unlock()

	buf, _ := mr.rateBuffer.GetAll()

	return buf
}
