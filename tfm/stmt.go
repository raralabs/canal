package tfm

import (
	"github.com/antonmedv/expr"
	"github.com/antonmedv/expr/vm"
)

type msgEnv struct {
}

func newMsgEnv() msgEnv {
	return msgEnv{}
}

type funcEnv map[string]interface{}

func newFuncEnv() funcEnv {
	env := make(funcEnv)
	env["str"] = func(val ...interface{}) interface{} {
		return "abc"
	}
	return env
}

type Stmt struct {
	stmt *vm.Program
}

func (stmt *Stmt) Run() (interface{}, error) {
	return expr.Run(stmt.stmt, newFuncEnv())
}

func NewStmt(stmt string) *Stmt {
	opts := []expr.Option{
		//expr.Env(newMsgEnv()),
		expr.Env(newFuncEnv()),
		expr.AllowUndefinedVariables(),
	}
	c, err := expr.Compile(stmt, opts...)
	if err != nil {
		panic(err.Error())
	}

	return &Stmt{stmt: c}
}
