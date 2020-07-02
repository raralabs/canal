package transforms

//
//import (
//	"github.com/d5/tengo"
//	"github.com/d5/tengo/stdlib"
//	"github.com/raralabs/canal/core/message"
//	"github.com/raralabs/canal/core/pipeline"
//	"github.com/raralabs/canal/transforms/base_transforms"
//)
//
//type Tengo struct {
//	script *tengo.Script
//}
//
//func NewTengo(src []byte) *Tengo {
//	t := &Tengo{}
//	t.Update(src)
//
//	return t
//}
//
//// Updates the script in Tengo, on the basis of the provided script
//// Can be called at runtime to update the script with new one
//func (t *Tengo) Update(src []byte) {
//
//	finalLine := []byte("\nOutput := Tengo(Input)")
//	src = append(src, finalLine...)
//
//	s := tengo.NewScript([]byte(src))
//	s.SetImports(stdlib.GetModuleMap(stdlib.AllModuleNames()...))
//
//	t.script = s
//}
//
//func (t *Tengo) Run(valueMap map[string]interface{}) map[string]interface{} {
//
//	// We expect the input of Tengo Function to be a map[string]interface{}
//	err := t.script.Add("Input", valueMap)
//	if err != nil {
//		panic(err)
//	}
//
//	c, err := t.script.Compile()
//	if err != nil {
//		panic(err)
//	}
//	c.Run()
//	output := c.Get("Output")
//	// We expect the output of Tengo Function to be a map[string]interface{}
//	return output.Map()
//}
//
//func (t *Tengo) handleMessage(m message.Msg) *message.MsgContent {
//	valueMap := make(map[string]interface{})
//	content := m.Content()
//	for k, v := range content {
//		// Eliminate or typecast the unsupported types by the TENGO
//		valueMap[k] = correctType(v.Value())
//	}
//
//	msg := make(message.MsgContent)
//	output := t.Run(valueMap)
//
//	for k, _ := range output {
//		msg.AddMessageValue(k, content[k])
//	}
//
//	return &msg
//}
//
//func (t *Tengo) TengoFunction() pipeline.Executor {
//	return base_transforms.NewDoOperator(func(m message.Msg) pipeline.ExeResp {
//		//msg := t.handleMessage(m)
//		return pipeline.ExeResp{}
//	})
//}
//
//func correctType(v interface{}) interface{} {
//	switch v := v.(type) {
//	case uint64:
//		// Tengo does not support unsigned integers
//		return int64(v)
//	}
//
//	return v
//}
