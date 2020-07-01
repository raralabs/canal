package transforms

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"github.com/raralabs/canal/core/pipeline"
//	"log"
//
//	"github.com/raralabs/canal/transforms/base_transforms"
//	"github.com/raralabs/canal/transforms/event/poll"
//	"github.com/raralabs/canal/transforms/join"
//	"github.com/raralabs/canal/transforms/join/table"
//)
//
//type Join struct {
//	tableFrom map[uint32]string
//	joiner    *join.Joiner
//	ev        *poll.CompositeEvent // The event that triggers the join output
//}
//
//// Provided descriptions must be in order: table1, table2, joined
//func NewJoin(event poll.Event,
//	desc []*table.Description,
//	fromId []uint32,
//	condition func(msg1, msg2 *message.MsgContent) bool) *Join {
//
//	if len(desc) != 3 {
//		log.Panicf("Need exactly 3 tables for join, got: %v", len(desc))
//		return nil
//	}
//	if len(fromId) != 2 {
//		log.Panicf("Need exactly 2 fromIds for join, got: %v", len(fromId))
//		return nil
//	}
//
//	jn := &Join{}
//
//	if ev, ok := event.(*poll.CompositeEvent); ok {
//		jn.ev = ev
//	} else {
//		jn.ev = poll.NewCompositeEvent("or", event)
//	}
//
//	tbl1 := table.NewTable(desc[0])
//	tbl2 := table.NewTable(desc[1])
//	joined := table.NewTable(desc[2])
//
//	jn.joiner = join.NewJoiner(tbl1, tbl2, joined, condition)
//
//	jn.tableFrom = make(map[uint32]string)
//	for i, v := range fromId {
//		jn.tableFrom[v] = desc[i].Alias
//	}
//
//	return jn
//}
//
//// start starts the join
//func (jn *Join) Start() {
//	jn.ev.Start()
//}
//
//// Reset resets the join
//func (jn *Join) Reset() {
//	jn.joiner.Reset()
//}
//
//// Fulfilling the functions for Join to act as an aggFunc
//
//func (jn *Join) trigger(*struct{}) bool {
//	// Check trigger for individual message not here
//	return true
//}
//
//func (jn *Join) toMessage(mf *message.Msg, s *struct{}) []*message.MsgContent {
//
//	var msgs []*message.MsgContent
//	msgVals := jn.joiner.Messages()
//
//	if len(msgVals) != 0 {
//		for _, mv := range msgVals {
//			// Check if the event has been triggered for the current messageValue
//			if jn.ev.Triggered(mv.Values()) {
//				msgs = append(msgs, &mv)
//			}
//		}
//	}
//	jn.ev.Reset()
//
//	return msgs
//}
//
//func (jn *Join) aggFunc(m *message.Msg, s *struct{}) (bool, error) {
//
//	//? How to aggregate
//	if v, ok := jn.tableFrom[m.GetStageId()]; ok {
//		jn.joiner.Insert(v, m.Content())
//	}
//
//	return true, nil
//}
//
//func (jn *Join) Function() pipeline.Executor {
//	var s struct{}
//	return base_transforms.NewAggOperator(s, jn.trigger, jn.toMessage, jn.aggFunc)
//}
