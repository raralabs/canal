package transforms

import (

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
	firstPath		string
	secondPath		string
	fields1  		[]string
	fields2  		[]string
	secondContainer []content.IContent
	mergeLock 		bool
}

func NewJoinProcessor(name string,joiner joinUsingHshMap.StreamJoin,query string) *joinProcessor {

	queryProcessor := joinqryparser.NewQueryParser(query)
	queryProcessor.PrepareQuery()
	fields1,fields2:=queryProcessor.Condition.Fields1,queryProcessor.Condition.Fields2
	return &joinProcessor{name:name,Joiner:joiner,query:query,firstPath:queryProcessor.FirstTable.Name,
							secondPath:queryProcessor.SecondTable.Name,fields1: fields1,fields2:fields2}
}

func (sp *joinProcessor)Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool{
	m := messagePod.Msg
	if pipeline.MsgRouteParam(sp.firstPath) == messagePod.Route {
		if m.Content().Keys()[0] != "eof" {
			sp.Joiner.ProcessStreamFirst(m.Content(), sp.fields1)
		} else {
			sp.mergeLock = true
		}
	}else if pipeline.MsgRouteParam(sp.secondPath) == messagePod.Route{
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
