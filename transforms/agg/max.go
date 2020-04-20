package agg

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"github.com/raralabs/canal/network/builder/utils"
//)
//
//// Max finds the max value in a stream if some conditions are satisfied
//type Max struct {
//	alias string                            // Alias of the max
//	filt  func(map[string]interface{}) bool // The filter function
//	field string
//}
//
//// NewCount creates a Max with the provided condition and returns it.
//func NewMax(alias, field string, f func(map[string]interface{}) bool) *Max {
//	return &Max{alias: alias, field: field, filt: f}
//}
//
//// Name returns the name of the Counter
//func (c *Max) Name() string {
//	return c.alias
//}
//
//// SetName sets the name of the Counter
//func (c *Max) SetName(alias string) {
//	c.alias = alias
//}
//
//// Aggregate counts the data based on the current value and the current
//// message
//func (c *Max) Aggregate(currentValue *message.MsgFieldValue, msg *message.MsgContent) *message.MsgFieldValue {
//	if c.filt != nil && currentValue == nil {
//		if !c.filt(msg.Values()) {
//			return nil
//		}
//	}
//
//	content := *msg
//	if currentValue == nil {
//		return content[c.field]
//	}
//
//	v := currentValue.Value()
//
//	switch currentValue.ValueType() {
//	case message.INT:
//		cmp, _ := utils.TryInt(v)
//		m, _ := utils.TryInt(content[c.field].Value())
//
//		mx := maxi(cmp, m)
//
//		return message.NewFieldValue(mx, message.INT)
//
//	case message.FLOAT:
//		cmp, _ := utils.TryFloat(v)
//		m, _ := utils.TryFloat(content[c.field].Value())
//
//		mx := maxf(cmp, m)
//
//		return message.NewFieldValue(mx, message.FLOAT)
//
//	}
//	return currentValue
//}
//
//// InitValue gives the initialization value for the max
//func (c *Max) InitValue() *message.MsgFieldValue {
//
//	return nil
//}
//
//// InitMsgValue gives the initialization value for the max based
//// on the message
//func (c *Max) InitMsgValue(msg message.MsgContent) *message.MsgFieldValue {
//
//	if c.filt != nil {
//		if !c.filt(msg.Values()) {
//			return c.InitValue()
//		}
//	}
//	return msg[c.field]
//}
//
//func (c *Max) Reset() {
//
//}
//
//func maxi(a, b int64) int64 {
//	if a > b {
//		return a
//	}
//	return b
//}
//
//func maxf(a, b float64) float64 {
//	if a > b {
//		return a
//	}
//	return b
//}
