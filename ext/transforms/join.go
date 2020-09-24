package transforms

import (
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/ext/transforms/joinUsingHshMap"
	"github.com/raralabs/canal/utils/joinqryparser"
)


//join processor is responsible for processing the query and then joining the streams
//joinProcessor contains all the necessary attributes for needed for the join
type joinProcessor struct{
	query  	 		string //query input form the user
	name    		string //name of the processor
	Joiner			joinUsingHshMap.StreamJoin //Join function to join the streams
	firstPath		string//equivalent to the left table
	secondPath		string//equivalent to the right table
	fields1  		[]string //condition fields for the join from table 1
	fields2  		[]string // condition fields for the join from table 2
	selectFields	[]string // select fields from the two tables
	secondContainer []content.IContent //container to hold the msg from second path while the first one is getting fed into the hash table
	mergeLock 		bool //merge lock enables to start merging once the streams of path1 are completely fed into the hash table
}

//create a processor that contains attributes for the join
func NewJoinProcessor(name string,joiner joinUsingHshMap.StreamJoin,query string) *joinProcessor {
	queryProcessor := joinqryparser.NewQueryParser(query)
	queryProcessor.PrepareQuery()
	fields1,fields2:=queryProcessor.Condition.Fields1,queryProcessor.Condition.Fields2
	selectFields := queryProcessor.Select
	fmt.Println(selectFields)
	return &joinProcessor{name:name,Joiner:joiner,query:query,firstPath:queryProcessor.FirstTable.Name,
		   				  secondPath:queryProcessor.SecondTable.Name,fields1: fields1,fields2:fields2,
		   				  selectFields: selectFields}
}

//executes the join
func (sp *joinProcessor)Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool{
	//extract msg from the pod
	m := messagePod.Msg
	//checks if the message is from first path or not if yes insert into the hash table till
	//eof is obtained
	if pipeline.MsgRouteParam(sp.firstPath) == messagePod.Route {
		//if streams from path1 are live
		if m.Content().Keys()[0] != "eof" {
			sp.Joiner.ProcessStreamFirst(m.Content(), sp.fields1)
		} else { //after eof is obtained set the mergelock to true for merging
			sp.mergeLock = true
		}
	//if stream is from second path then insert into the array till incoming msg from
	//path1 is complete 
	}else if pipeline.MsgRouteParam(sp.secondPath) == messagePod.Route{
		if m.Content().Keys()[0] !="eof" {
			sp.secondContainer = append(sp.secondContainer, m.Content())
		}
	}
	//var previous_content content.IContent = nil
	//if mergelock is true start merging the streams
	if sp.mergeLock ==true{
		for _,msg := range sp.secondContainer {
			result,ok := sp.Joiner.ProcessStreamSec(msg,sp.fields2)
			if ok{
				merged := sp.Joiner.Join(result.(content.IContent),msg,sp.selectFields)
				proc.Result(m,merged,nil)
				//previous_content = merged
			}
		}
		sp.secondContainer = nil
		}
	return true
}
//after the join processor is complete insert eof and closed it
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
