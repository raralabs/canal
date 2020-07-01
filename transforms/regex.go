package transforms

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/transforms/base_transforms"
	"log"
	"regexp"
)

func RegExp(exp, key string, f func(*regexp.Regexp, string) string) pipeline.Executor {

	reg, err := regexp.Compile(exp)

	if err != nil {
		log.Fatalf("Could not parse regular expression: %s", exp)
	}

	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

		content := m.Content()
		types := m.Types()

		str := content[key]

		if types[key] != message.STRING {
			log.Fatalf("Could not parse non-string values: %v", str)
		}

		st,_ := str.Val.(string)

		if reg.MatchString(st) {
			s := f(reg, st)
			str.Val = s

			content[key] = str
		}

		proc.Result(m, content)
		return true
	}

	return base_transforms.NewDoOperator(df)
}
