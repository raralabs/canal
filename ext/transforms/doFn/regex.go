package doFn

import (
	"github.com/raralabs/canal/core/message"
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"log"
	"regexp"
)



func RegExParser(exp, key string, f func(*regexp.Regexp, string) map[string]string) pipeline.Executor {
	reg, err := regexp.Compile(exp)
	if err != nil {
		log.Panicf("Could not parse regular expression: %s", exp)
	}
	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		contents := m.Content().Copy()
		types := m.Types()
		if v, ok := contents.Get("eof"); ok {
			if v.Val == true {
				proc.Result(m, contents, nil)
				return true
			}
		}
		str, _ := contents.Get(key)
		if types[key] != content2.STRING {
			log.Panicf("Could not parse non-string values: %v", str)
		}
		st, _ := str.Val.(string)
		s := f(reg, st)
		var digitCheck = regexp.MustCompile(`^[0-9]+$`)
		contents.Remove(key)
		for key,value := range (s){
			yes := digitCheck.MatchString(string(value))
			str.Val = value
			if yes{
				str.ValType = content2.INT
			}else{
				str.ValType = content2.STRING
			}
			contents.Add(key, str)
		}
		proc.Result(m, contents, nil)
		return true
	}

	return do.NewOperator(df)
}

func RegExp(exp, key string, f func(*regexp.Regexp, string) string) pipeline.Executor {
	reg, err := regexp.Compile(exp)
	if err != nil {
		log.Panicf("Could not parse regular expression: %s", exp)
	}

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		content := m.Content()
		types := m.Types()
		if v, ok := content.Get("eof"); ok {
			if v.Val == true {
				proc.Result(m, content, nil)
				return true
			}
		}
		str, _ := content.Get(key)
		if types[key] != content2.STRING {
			log.Panicf("Could not parse non-string values: %v", str)
		}
		st, _ := str.Val.(string)

		//if reg.MatchString(st) {
			s := f(reg, st)
			str.Val = s

			content.Add(key, str)
		//}
		proc.Result(m, content, nil)
		return true
	}

	return do.NewOperator(df)
}
