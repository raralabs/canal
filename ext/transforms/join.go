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
	name    		string //name of the processor
	Joiner			joinUsingHshMap.StreamJoin //Join function to join the streams
	fields1  		[]string //condition fields for the join from table 1
	fields2  		[]string // condition fields for the join from table 2

}
//this is the older version
//create a processor that contains attributes for the join
//func NewJoinProcessor(name string,query string) *joinProcessor {
//	queryProcessor := joinqryparser.NewQueryParser(query)
//	queryProcessor.PrepareQuery()
//	fields1, fields2 := queryProcessor.Condition.Fields1, queryProcessor.Condition.Fields2
//	selectFields, joinType := queryProcessor.Select, queryProcessor.JoinType
//	fmt.Println("jointype",joinType)
//	switch joinType {
//	case joinUsingHshMap.INNER:
//		joiner := joinUsingHshMap.NewInnerJoin(joinUsingHshMap.HASH, queryProcessor.FirstTable.Name,queryProcessor.SecondTable.Name,selectFields)
//		return &joinProcessor{name: name, Joiner: joiner, query: query, fields1: fields1, fields2: fields2}
//	case joinUsingHshMap.OUTER:
//		joiner := joinUsingHshMap.NewOuterJoin(joinUsingHshMap.HASH, queryProcessor.FirstTable.Name,queryProcessor.SecondTable.Name,selectFields,queryProcessor.SubType)
//		return &joinProcessor{name: name, Joiner: joiner, query: query, fields1: fields1, fields2: fields2}
//	default:
//		joiner := joinUsingHshMap.NewInnerJoin(joinUsingHshMap.HASH, queryProcessor.FirstTable.Name,queryProcessor.SecondTable.Name,selectFields)
//		return &joinProcessor{name: name, Joiner: joiner, query: query, fields1: fields1, fields2: fields2}
//	}
//}
//new version
//creates the join processor
func NewJoinProcessor(name string,fields1,fields2,selectFields []string,joinType joinUsingHshMap.JoinType,firstTableName,secondTableName string) (*joinProcessor,error) {
	switch joinType {
	case joinUsingHshMap.INNER:
		joiner := joinUsingHshMap.NewInnerJoin(joinUsingHshMap.HASH, firstTableName,secondTableName,selectFields)
		return &joinProcessor{name: name, Joiner: joiner, fields1: fields1, fields2: fields2},nil
	case joinUsingHshMap.OUTER:
		joiner := joinUsingHshMap.NewOuterJoin(joinUsingHshMap.HASH, firstTableName,secondTableName,selectFields,"")
		return &joinProcessor{name: name, Joiner: joiner,fields1: fields1, fields2: fields2},nil
	default:
		panic("unsupported join type ")
	}
}

//executes the join
func (sp *joinProcessor)Execute(messagePod pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool{
	success := sp.Joiner.Join(messagePod,sp.fields1,sp.fields2,proc)
	return success
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
