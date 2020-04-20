package base_transforms

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"github.com/raralabs/canal/core/pipeline"
//)
//
//type AggOperator struct {
//	name    string
//	state   *struct{}
//	trigger func(*struct{}) bool
//	toMsg   func(*message.Msg, *struct{}) []*message.MsgContent
//	aggFunc func(*message.Msg, *struct{}) (bool, error)
//}
//
//func NewAggOperator(
//	initialState struct{},
//	tf func(*struct{}) bool,
//	tmf func(*message.Msg, *struct{}) []*message.MsgContent,
//	af func(*message.Msg, *struct{}) (bool, error),
//) pipeline.Executor {
//	return &AggOperator{
//		state:   &initialState,
//		trigger: tf,
//		toMsg:   tmf,
//		aggFunc: af,
//	}
//}
//
//func (af *AggOperator) Execute(m message.Msg) pipeline.ExeResp {
//	//res, err := af.aggFunc(m, af.state)
//	//
//	//if err != nil || !af.trigger(af.state) {
//	//	return nil, res, err
//	//}
//	//
//	//output := af.toMsg(mf, af.state)
//	//if output == nil {
//	//	return nil, res, err
//	//}
//	return pipeline.ExeResp{}
//}
//
//func (*AggOperator) ExecutorType() pipeline.ExecutorType {
//	return pipeline.TRANSFORM
//}
//
//func (*AggOperator) HasLocalState() bool {
//	return true
//}
//
//func (af *AggOperator) SetName(name string) {
//	af.name = name
//}
//
//func (af *AggOperator) Name() string {
//	return af.name
//}
