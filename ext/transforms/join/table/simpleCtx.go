package table

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"log"
//)
//
//// SimpleContext is responsible for defining:
////	a) if the combinations for the fields in the table should be unique.
////	a) if some default value should be used if there are missing values in the
////	   message for the required fields in the table.
////	c) if some condition is to be satisfied by all the elements in some column of
////	   the table.
////	d) if some column in the table should hold only unique elements.
//type SimpleContext struct {
//	uniq  bool
//	cuniq []string
//	ccnd  func([]message.MsgFieldValue) bool
//
//	defaultValue message.MsgContent
//}
//
//func NewSimpleContext(unique bool, colUnique []string, colCondition func([]message.MsgFieldValue) bool,
//	defaultValue message.MsgContent) *SimpleContext {
//
//	if defaultValue == nil {
//		defaultValue = make(message.MsgContent)
//	}
//
//	ctx := &SimpleContext{uniq: unique, cuniq: colUnique, ccnd: colCondition, defaultValue: defaultValue}
//
//	return ctx
//}
//
//// Insertable cheks if the given message value is insertable to the
//// table on the basis of it's context
//func (ctx *SimpleContext) Insertable(tbl Table, msg message.MsgContent) bool {
//
//	fields := tbl.Fields()
//
//	if len(fields) == 0 {
//		return false
//	}
//
//	data := make(message.MsgContent)
//	for _, fld := range fields {
//		if v, ok := msg[fld]; ok {
//			data[fld] = v
//		} else if v, ok := ctx.defaultValue[fld]; ok {
//			data[fld] = v
//		} else {
//			log.Panicf("Table could not be filled for: %v", fld)
//			return false
//		}
//	}
//
//	if ctx.uniq {
//		for x := range tbl.IterRows() {
//			out := true
//
//			for i, v := range fields {
//				v1 := data[v].Value()
//				out = out && (v1 == x[i].Value())
//			}
//
//			if out {
//				return false
//			}
//		}
//	}
//
//	if len(ctx.cuniq) != 0 {
//		for x := range tbl.IterRows() {
//			out := true
//
//			for i, v := range fields {
//				for _, b := range ctx.cuniq {
//					if v == b {
//						v1 := data[v].Value()
//						out = out && (v1 == x[i].Value())
//					}
//				}
//			}
//
//			if out {
//				return false
//			}
//		}
//	}
//
//	return true
//}
//
//// Insert inserts a message to the table passed to it.
//func (ctx *SimpleContext) Insert(tbl Table, msg message.MsgContent) {
//
//	fields := tbl.Fields()
//
//	data := make(message.MsgContent)
//	for _, fld := range fields {
//		if v, ok := msg[fld]; ok {
//			data[fld] = v
//		} else if v, ok := ctx.defaultValue[fld]; ok {
//			data[fld] = v
//		} else {
//			log.Panicf("Table could not be filled for: %v", fld)
//			return
//		}
//	}
//
//	// Simply append to the end of the table
//	vals := make([]message.MsgFieldValue, len(fields))
//	for i, v := range fields {
//		vals[i] = data[v]
//	}
//	tbl.Append(vals)
//}
