package pipeline

import (
	"time"

	"github.com/raralabs/canal/core/message"
)

type msgPod struct {
	msg   message.Msg
	route msgRouteParam
}

func newMsgPod(msg message.Msg) msgPod {
	return msgPod{msg: msg, route: ""}
}

type sendRoute struct {
	sendChannel chan msgPod
	route       msgRouteParam
	timer       *time.Timer
	retries     uint8
}

func newSendRoute(ch chan msgPod, r msgRouteParam) sendRoute {
	timer := time.NewTimer(1 * time.Minute)
	timer.Stop()

	return sendRoute{sendChannel: ch, route: r, timer: timer}
}

func (r *sendRoute) send(m message.Msg, timeout time.Duration, onTimeout func() bool) bool {
	pod := msgPod{msg: m, route: r.route}
	r.timer.Reset(timeout)
	sent := false
sendLoop:
	for {
		select {
		case <-r.timer.C:
			r.retries++
			if onTimeout() || r.retries >= 2 {
				println("[Timeout] tried for ", r.retries, " times in ", r.route)
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
