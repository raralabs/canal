package transforms

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"github.com/raralabs/canal/core/pipeline"
//	"github.com/raralabs/canal/transforms/base_transforms"
//	"github.com/raralabs/goEmbed/lua"
//)
//
//type GopherLua struct {
//	script *lua.LuaScript
//}
//
//func NewGopherLua(src []byte) *GopherLua {
//	gl := &GopherLua{}
//	gl.Update(src)
//
//	return gl
//}
//
//// Updates the script in GopherLua, on the basis of the provided script
//// Can be called at runtime to update the script with new one
//func (gl *GopherLua) Update(src []byte) {
//
//	finalLine := []byte("\nreturn GopherLua(Input)")
//	src = append(src, finalLine...)
//
//	gl.script = lua.NewLuaScript(src)
//
//	// Open any required libraries or add funcs to appropriate library to
//	// gl.script here
//
//}
//
//func (gl *GopherLua) Run(valueMap map[string]interface{}) map[string]interface{} {
//
//	gl.script.SetGlobalMap("Input", valueMap)
//
//	output := gl.script.RunGetMap()
//
//	return output
//}
//
//func (gl *GopherLua) handleMessage(m message.Msg) *message.MsgContent {
//	valueMap := make(map[string]interface{})
//	content := m.Content()
//	for k, v := range content {
//		// Eliminate or typecast the unsupported types by the TENGO
//		valueMap[k] = v.Value()
//	}
//
//	msg := make(message.MsgContent)
//	output := gl.Run(valueMap)
//
//	for k, _ := range output {
//		msg.AddMessageValue(k, content[k])
//	}
//	return &msg
//}
//
//func (gl *GopherLua) GopherLuaFunction() pipeline.Executor {
//	return base_transforms.NewDoOperator(func(m message.Msg) pipeline.ExeResp {
//		//msg := gl.handleMessage(m)
//		return pipeline.ExeResp{}
//	})
//}
