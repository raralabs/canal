package doFn

import (
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
)

type BatchMaker struct {
	firstRecord bool
	header      []string
	records     map[string][]*message.MsgFieldValue
	doneCheck   func(m message.Msg) bool
	doneFunc    func(m message.Msg, proc pipeline.IProcessorForExecutor, contents []*message.OrderedContent)
}

func NewBatchMaker(doneCheck func(m message.Msg) bool,
	doneFunc func(m message.Msg, proc pipeline.IProcessorForExecutor, contents []*message.OrderedContent)) *BatchMaker {
	return &BatchMaker{
		firstRecord: true,
		records:     make(map[string][]*message.MsgFieldValue),
		doneCheck:   doneCheck,
		doneFunc:    doneFunc,
	}
}

func (cw *BatchMaker) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

	if cw.doneCheck(m) {
		if len(cw.header) != 0 {
			depth := len(cw.records[cw.header[0]])
			contents := make([]*message.OrderedContent, depth)
			for i := 0; i < depth; i++ {
				content := message.NewOrderedContent()
				for _, h := range cw.header {
					content.Add(h, cw.records[h][i])
				}
				contents[i] = content
			}

			cw.doneFunc(m, proc, contents)
		}
		return false
	}

	content := m.Content()
	if cw.firstRecord {
		cw.header = make([]string, content.Len())
		i := 0
		for e := content.First(); e != nil; e = e.Next() {
			k, _ := e.Value.(string)
			cw.header[i] = k
			i++
		}
		cw.firstRecord = false
	}

	depth := 0
	if len(cw.header) != 0 {
		depth = len(cw.records[cw.header[0]])
	}

	pContent := m.PrevContent()
	if pContent != nil {
		for i := 0; i < depth; i++ {
			replace := true
			for _, h := range cw.header {
				v1, ok1 := cw.records[h]
				v2, ok2 := pContent.Get(h)

				if !ok1 && !ok2 {
					goto breakLoop
				}

				if !(v1[i].Value() == v2.Value() && v1[i].ValueType() == v2.ValueType()) {
					replace = false
				}

				if !replace {
					break
				}
			}

			if replace {
				for _, h := range cw.header {
					val, _ := content.Get(h)
					cw.records[h][i] = val
				}
				return false
			}
		}
	}
breakLoop:
	for _, k := range cw.header {
		if val, ok := content.Get(k); ok {
			cw.records[k] = append(cw.records[k], val)
		}
	}

	return false
}

func BatchFunction(doneCheck func(m message.Msg) bool,
	doneFunc func(m message.Msg, proc pipeline.IProcessorForExecutor, contents []*message.OrderedContent)) pipeline.Executor {

	batches := NewBatchMaker(doneCheck, doneFunc)

	return do.NewOperator(func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		return batches.Execute(m, proc)
	})
}
