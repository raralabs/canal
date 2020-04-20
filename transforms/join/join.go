package join

//
//import (
//	"fmt"
//	"github.com/raralabs/canal/core/message"
//	"log"
//	"sync"
//
//	"github.com/raralabs/canal/transforms/join/table"
//)
//
//type Joiner struct {
//	table1 table.Table
//	table2 table.Table
//
//	joinedMu *sync.Mutex
//	joined   table.Table
//
//	condition func(msg1, msg2 *message.MsgContent) bool
//}
//
//func NewJoiner(table1, table2, joined table.Table, condition func(msg1, msg2 *message.MsgContent) bool) *Joiner {
//
//	if table1.Alias() == table2.Alias() {
//		log.Fatalf("Both tables have same alias: %s", table1.Alias())
//		return nil
//	}
//
//	mergedFields := MergeFields(table1.Alias(), table2.Alias(), table1.Fields(), table2.Fields())
//	joined.AddFields(mergedFields...)
//
//	return &Joiner{table1: table1,
//		table2:    table2,
//		joined:    joined,
//		condition: condition,
//		joinedMu:  &sync.Mutex{},
//	}
//}
//
//func (j *Joiner) Insert(tableName string, msgVal *message.MsgContent) {
//
//	var tbl, otherTbl table.Table
//	var t1Msg, t2Msg *message.MsgContent
//
//	if tableName == j.table1.Alias() {
//		tbl = j.table1
//		otherTbl = j.table2
//		t1Msg = msgVal
//
//	} else if tableName == j.table2.Alias() {
//		tbl = j.table2
//		otherTbl = j.table1
//		t2Msg = msgVal
//
//	} else {
//		log.Fatalf("No table with alias = %s", tableName)
//		return
//	}
//
//	if tbl.Insertable(msgVal) {
//		fields := otherTbl.Fields()
//		tblMsgVal := make(message.MsgContent)
//
//		j.joinedMu.Lock()
//		for x := range otherTbl.IterRows() {
//			for i, v := range fields {
//				tblMsgVal[v] = x[i]
//			}
//
//			if tableName == j.table1.Alias() {
//				t2Msg = &tblMsgVal
//			} else if tableName == j.table2.Alias() {
//				t1Msg = &tblMsgVal
//			}
//
//			if j.condition(t1Msg, t2Msg) {
//				// Join the values
//				merged := MergeMessages(j.table1.Alias(),
//					j.table2.Alias(),
//					t1Msg,
//					t2Msg)
//
//				if j.joined.Insertable(merged) {
//					j.joined.Insert(merged)
//				}
//			}
//		}
//		j.joinedMu.Unlock()
//
//		// Insert to the corresponding table
//		tbl.Insert(msgVal)
//	}
//}
//
//func (j *Joiner) Messages() []message.MsgContent {
//	j.joinedMu.Lock()
//	defer j.joinedMu.Unlock()
//
//	if j.joined.Len() == 0 {
//		// No Messages have been inserted
//		return []message.MsgContent{}
//	}
//
//	depth := j.joined.Len()
//	msgs := make([]message.MsgContent, depth)
//	fields := j.joined.Fields()
//
//	k := 0
//	for x := range j.joined.IterRows() {
//		msg := make(message.MsgContent)
//		for i, v := range fields {
//			msg[v] = x[i]
//		}
//
//		msgs[k] = msg
//		k++
//	}
//
//	return msgs
//}
//
//func (j *Joiner) Reset() {
//}
//
//func MergeMessages(a1, a2 string, m1, m2 *message.MsgContent) *message.MsgContent {
//	merged := make(message.MsgContent)
//
//	for k, v := range *m1 {
//		key := fmt.Sprintf("%s.%s", a1, k)
//		merged[key] = v
//	}
//
//	for k, v := range *m2 {
//		key := fmt.Sprintf("%s.%s", a2, k)
//		merged[key] = v
//	}
//
//	return &merged
//}
//
//func MergeFields(a1, a2 string, f1, f2 []string) []string {
//	fields := make([]string, len(f1)+len(f2))
//
//	i := 0
//	for _, v := range f1 {
//		fields[i] = fmt.Sprintf("%s.%s", a1, v)
//		i++
//	}
//
//	for _, v := range f2 {
//		fields[i] = fmt.Sprintf("%s.%s", a2, v)
//		i++
//	}
//
//	return fields
//}
