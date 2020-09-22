package transforms

import (
	"fmt"
	//"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/transforms/joinUsingHshMap"
	"github.com/raralabs/canal/utils/joinqryparser"
)

type joinProcessor struct{
	query  	 		string
	name    		string
	Joiner			joinUsingHshMap.StreamJoin
	route 			string
	fields1  		[]string
	fields2  		[]string
	firstProcessor 	uint32
	secondContainer []content.IContent
	mergeLock 		bool
}

func NewJoinProcessor(name string,joiner joinUsingHshMap.StreamJoin,query string) *joinProcessor {
	qr:= "SELECT table,chair FROM table1 c INNERJOIN table2 d on table1.id=table2.id"
	queryProcessor := joinqryparser.NewQueryParser(qr)
	queryProcessor.PrepareQuery()
	fields1,fields2:= joiner.Condition(query)
	return &joinProcessor{name:name,Joiner:joiner,query:query,fields1: fields1,fields2:fields2}
}

func (sp *joinProcessor)Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool{
	if sp.firstProcessor==0{
		sp.firstProcessor = m.ProcessorId()
	}
	if m.ProcessorId() == sp.firstProcessor {
		if m.Content().Keys()[0] != "eof" {
			sp.Joiner.ProcessStreamFirst(m.Content(), sp.fields1)
		} else {
			sp.mergeLock = true
		}
	}else{
		if m.Content().Keys()[0] !="eof" {
			sp.secondContainer = append(sp.secondContainer, m.Content())
		}
	}
	var previous_content content.IContent = nil
	if sp.mergeLock ==true{
		for _,msg := range sp.secondContainer {
			result,ok := sp.Joiner.ProcessStreamSec(msg,sp.fields2)
			if ok{
				merged := sp.Joiner.Join(result.(content.IContent),msg)
				proc.Result(m,merged,previous_content)
				previous_content = merged
			}

		}
		sp.secondContainer = nil
		}
	return true
}

func (sp *joinProcessor) done(m message.Msg, proc pipeline.IProcessorForExecutor) {
	// Send eof if done
	msgContents := content.New()
	msgContents = msgContents.Add("eof", content.NewFieldValue(true, content.BOOL))
	proc.Result(m, msgContents, nil)
	proc.Done()
}

func (sp *joinProcessor) ExecutorType() pipeline.ExecutorType {
	return pipeline.TRANSFORM
}


func (sp *joinProcessor) HasLocalState() bool {
	return false
}

func (sp *joinProcessor) SetName(name string) {
	sp.name = name
}

func (sp *joinProcessor) Name() string {
	return sp.name
}
