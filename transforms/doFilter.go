package transforms

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"github.com/raralabs/canal/core/pipeline"
//	"github.com/raralabs/canal/transforms/base_transforms"
//)
//
//func FilterFunction(filter func(m message.Msg) (bool, error)) pipeline.Executor {
//	return base_transforms.NewDoOperator(func(m message.Msg) pipeline.ExeResp {
//		//var respM []message.MsgContent = nil
//		//
//		//match, err := filter(m)
//		//if err == nil && match {
//		//	c := m.Content()
//		//	respM = []message.MsgContent{c}
//		//}
//
//		return pipeline.ExeResp{}
//	})
//}
