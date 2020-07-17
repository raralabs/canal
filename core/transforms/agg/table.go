package agg

import (
	"errors"
	"fmt"
	"github.com/raralabs/canal/core/message"
	"log"
	"strings"
)

func stringRep(strs ...interface{}) string {
	var str strings.Builder

	for _, s := range strs {
		str.WriteString(fmt.Sprintf("%v", s))
	}

	return str.String()
}

func getStringRep(groupVals []*message.MsgFieldValue) string {
	values := make([]interface{}, len(groupVals))
	for i, v := range groupVals {
		values[i] = v.Val
	}

	return stringRep(values...)
}

func extractValues(m *message.OrderedContent, header []string) ([]*message.MsgFieldValue, error) {
	groupVals := make([]*message.MsgFieldValue, len(header))
	for i, grp := range header {
		if v, ok := m.Get(grp); ok {
			groupVals[i] = v
		} else {
			return nil, errors.New("required contents unavailable")
		}
	}
	return groupVals, nil
}

type Table struct {
	groupBy     []string              // The groups in the table
	aggFns      map[string][]IAggFunc // The aggregators for each group
	aggFnTmplts []IAggFuncTemplate
	table       map[string][]*message.MsgFieldValue
}

func NewTable(aggs []IAggFuncTemplate, groupBy ...string) *Table {
	table := make(map[string][]*message.MsgFieldValue)
	groups := make([]string, len(groupBy))
	for i, s := range groupBy {
		groups[i] = s
	}

	aggFnTmplts := make([]IAggFuncTemplate, len(aggs))
	for i, ag := range aggs {
		aggFnTmplts[i] = ag
	}

	aggFns := make(map[string][]IAggFunc)

	return &Table{
		groupBy:     groups,
		aggFns:      aggFns,
		aggFnTmplts: aggFnTmplts,
		table:       table,
	}
}

// Insert inserts the content to the table and generates at max
// two contents and prevContents for message to be generated.
// One pair is for the update info on removal and the other is
// the info on addition.
func (t *Table) Insert(content, prevContent *message.OrderedContent) ([]*message.OrderedContent, []*message.OrderedContent, error) {

	groupVals, err := extractValues(content, t.groupBy)
	if err != nil {
		return nil, nil, err
	}
	strRep := getStringRep(groupVals)

	var pContent *message.OrderedContent
	var pContentRem, contentRem *message.OrderedContent

	// Skip the insertion and removal of contents, if current content and
	// the previous content is identical.
	if prevContent != content {

		// If previous content is available, handle it appropriately
		if prevContent != nil {

			values, err := extractValues(prevContent, t.groupBy)
			if err != nil {
				return nil, nil, err
			}
			prevStrRep := getStringRep(values)

			// Check if previous content exists in table
			if _, ok := t.table[prevStrRep]; ok {

				pContentRem = message.NewOrderedContent()
				contentRem = message.NewOrderedContent()
				// Insert group info to the content
				t.fillGroupInfo(pContentRem, prevStrRep)
				t.fillGroupInfo(contentRem, prevStrRep)

				// Collect contents before removal
				t.collectResults(pContentRem, prevStrRep)
				for _, aggFn := range t.aggFns[prevStrRep] {
					aggFn.Remove(prevContent)
				}
				// Collect contents after removal
				t.collectResults(contentRem, prevStrRep)

			} else {
				return nil, nil, errors.New("previous content vanished from table")
			}
		}

		if _, ok := t.table[strRep]; ok {

			pContent = message.NewOrderedContent()
			// Insert group info to the content, and collect
			// results.
			t.fillGroupInfo(pContent, strRep)
			t.collectResults(pContent, strRep)

			for _, aggFn := range t.aggFns[strRep] {
				aggFn.Add(content)
			}
		} else {
			// Fill the table with new elements
			t.table[strRep] = groupVals

			// Create new aggregator functions for the group
			aggs := make([]IAggFunc, len(t.aggFnTmplts))
			for i, tmplt := range t.aggFnTmplts {
				aggs[i] = tmplt.Function()
			}
			t.aggFns[strRep] = aggs

			// Add the content to the aggregator functions
			for _, aggFn := range t.aggFns[strRep] {
				aggFn.Add(content)
			}
		}
	}

	newContent := message.NewOrderedContent()
	// Insert group info and results to the newContent
	t.fillGroupInfo(newContent, strRep)
	t.collectResults(newContent, strRep)

	var nCs, pCs []*message.OrderedContent
	if pContentRem != nil && contentRem != nil {
		// Place removed content at the beginning
		pCs = []*message.OrderedContent{pContentRem}
		nCs = []*message.OrderedContent{contentRem}
	}
	// Place added content at the end
	pCs = append(pCs, pContent)
	nCs = append(nCs, newContent)

	return nCs, pCs, nil
}

// Entries provides a way to access the table's content.
// It returns a slice that contains groups info and
// aggregator functions' results.
func (t *Table) Entries() []*message.OrderedContent {
	var contents []*message.OrderedContent

	for k, _ := range t.table {
		content := message.NewOrderedContent()

		// Insert group info to the content
		t.fillGroupInfo(content, k)

		// Insert aggregator functions' results to the content
		t.collectResults(content, k)

		contents = append(contents, content)
	}

	return contents
}

func (t *Table) Reset() {
	panic("implement me")
}

// fillGroupInfo fills the group info for the provided
// group string.
func (t *Table) fillGroupInfo(m *message.OrderedContent, grpStr string) {
	values := t.table[grpStr]
	if len(t.groupBy) != len(values) {
		log.Panic("Error in filling group info.")
	}
	for i, grp := range t.groupBy {
		m.Add(grp, values[i])
	}
}

// collectResults collects the aggregator results for the
// provided group string.
func (t *Table) collectResults(m *message.OrderedContent, grpStr string) {
	aggs := t.aggFns[grpStr]
	for _, ag := range aggs {
		m.Add(ag.Name(), ag.Result())
	}
}
