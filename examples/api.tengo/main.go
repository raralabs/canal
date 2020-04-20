package main

import (
	"github.com/d5/tengo"
	"github.com/d5/tengo/stdlib"
	"github.com/raralabs/canal/core/message"
)

type TMes struct {
	tengo.ObjectImpl
	Value *message.Msg
}

func (t *TMes) TypeName() string {
	return "TMes"
}

func (t *TMes) IndexGet(index tengo.Object) (tengo.Object, error) {
	return &tengo.String{Value: "What"}, nil
}

type TResult struct {
	tengo.ObjectImpl
	val interface{}
}

func (t *TResult) TypeName() string {
	return "retval"
}

func (t *TResult) String() string {
	if t.val == nil {
		return ""
	}

	return t.val.(tengo.Object).String()
}

func (r *TResult) Call(args ...tengo.Object) (ret tengo.Object, err error) {
	if len(args) != 1 {
		err = tengo.ErrWrongNumArguments
	}
	r.val = args[0]
	return
}

func (r *TResult) CanCall() bool {
	return true
}

func main() {
	script := tengo.NewScript([]byte(`
		math := import("math") 
		retval(int(msg.value) == undefined ? false : int(msg.value) % 2 == 0)
	`))
	script.SetImports(stdlib.GetModuleMap("math"))
	err := script.Add("msg", &TMes{Value: &message.Msg{}})
	if err != nil {
		panic(err)
	}
	err = script.Add("retval", &TResult{})
	if err != nil {
		panic(err)
	}

	compiled, err := script.Compile()
	if err != nil {
		panic(err)
	}

	err = compiled.Run()
	if err != nil {
		panic(err)
	}

	println(compiled.Get("retval").Value().(*TResult).String())
}
