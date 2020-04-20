package pipeline

import (
	"github.com/raralabs/canal/core/message"
	"time"
)

type msgPod struct {
	msg       message.Msg
	routeName string
}

func newMsgPod(msg message.Msg) msgPod {
	return msgPod{msg: msg, routeName: ""}
}

type sendRoute struct {
	sendChannel chan msgPod
	routeName   string
	timer       *time.Timer
	retries     uint8
}

func newSendRoute(ch chan msgPod, r string) sendRoute {
	timer := time.NewTimer(1 * time.Minute)
	timer.Stop()

	return sendRoute{sendChannel: ch, routeName: r, timer: timer}
}

func (r *sendRoute) send(m message.Msg, timeout time.Duration, onTimeout func() bool) bool {
	pod := msgPod{msg: m, routeName: r.routeName}
	r.timer.Reset(timeout)
	sent := false
sendLoop:
	for {
		select {
		case <-r.timer.C:
			r.retries++
			if onTimeout() || r.retries >= 2 {
				println("[Timeout] tried for ", r.retries, " times in ", r.routeName)
				break sendLoop
			}
			r.timer.Reset(timeout)
		case r.sendChannel <- pod:
			sent = true
			r.retries = 0
			break sendLoop
		}
	}

	r.timer.Stop()
	return sent
}
