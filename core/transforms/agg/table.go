package agg

import (
	"errors"
	"fmt"
	"github.com/raralabs/canal/core/message"
	stream_math "github.com/raralabs/canal/utils/stream-math"
	"strings"
)

func stringRep(strs ...interface{}) string {
	var str strings.Builder

	for _, s := range strs {
		str.WriteString(fmt.Sprintf("%v", s))
	}

	return str.String()
}

type Table struct {
	groupBy     []string              // The groups in the table
	aggFns      map[string][]IAggFunc // The aggregators for each group
	aggFnTmplts []IAggFuncTemplate
	table       map[string][]*message.MsgFieldValue
	msgFreq     *stream_math.FreqCounter
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
		msgFreq:     stream_math.NewFreqCounter(),
	}
}

func (t *Table) Insert(content, prevContent *message.OrderedContent) (*message.OrderedContent, *message.OrderedContent, error) {
	groupVals := make([]*message.MsgFieldValue, len(t.groupBy))
	for i, grp := range t.groupBy {
		if v, ok := content.Get(grp); ok {
			groupVals[i] = v
		} else {
			return nil, nil, errors.New("required contents unavailable")
		}
	}
	values := make([]interface{}, len(groupVals))
	for i, v := range groupVals {
		values[i] = v.Val
	}
	strRep := stringRep(values...)

	gotPContent := false
	var pContent *message.OrderedContent

	if prevContent != nil {
		prevGroupVals := make([]*message.MsgFieldValue, len(t.groupBy))
		for i, grp := range t.groupBy {
			if v, ok := prevContent.Get(grp); ok {
				prevGroupVals[i] = v
			} else {
				return nil, nil, errors.New("required contents unavailable")
			}
		}
		prevValues := make([]interface{}, len(prevGroupVals))
		for i, v := range prevGroupVals {
			prevValues[i] = v.Val
		}

		prevStrRep := stringRep(prevValues...)

		// Check if previous content exists in table
		if vals, ok := t.table[prevStrRep]; ok && prevStrRep != strRep {
			if t.msgFreq.Remove(prevStrRep) != nil {
				// Collect previous values
				pContent = message.NewOrderedContent()
				// Insert group info to the content
				for i, grp := range t.groupBy {
					pContent.Add(grp, vals[i])
				}
				gotPContent = true

				for _, ag := range t.aggFns[prevStrRep] {
					ag.Remove(prevContent)
				}

				// Replace with new content
				t.table[strRep] = groupVals
				delete(t.table, prevStrRep)
				// Update the aggregators
				t.aggFns[strRep] = t.aggFns[prevStrRep]
				delete(t.aggFns, prevStrRep)
			}
		}
	}

	if vals, ok := t.table[strRep]; ok {
		// Extract current agg content of the table and
		// Add the content to the aggregator functions
		if !gotPContent {
			pContent = message.NewOrderedContent()
			// Insert group info to the content
			for i, grp := range t.groupBy {
				pContent.Add(grp, vals[i])
			}
		}

		for _, aggFn := range t.aggFns[strRep] {
			pContent.Add(aggFn.Name(), aggFn.Result())
			aggFn.Add(content, prevContent)
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
			aggFn.Add(content, prevContent)
		}
	}
	t.msgFreq.Add(strRep)

	newContent := message.NewOrderedContent()
	vals := t.table[strRep]

	// Insert group info to the content
	for i, grp := range t.groupBy {
		newContent.Add(grp, vals[i])
	}

	// Insert aggregator functions' results to the content
	aggs := t.aggFns[strRep]
	for _, ag := range aggs {
		newContent.Add(ag.Name(), ag.Result())
	}

	return newContent, pContent, nil
}

func (t *Table) Entries() []*message.OrderedContent {
	var contents []*message.OrderedContent

	for k, v := range t.table {
		content := message.NewOrderedContent()

		// Insert group info to the content
		for i, grp := range t.groupBy {
			content.Add(grp, v[i])
		}

		// Insert aggregator functions' results to the content
		aggs := t.aggFns[k]
		for _, ag := range aggs {
			content.Add(ag.Name(), ag.Result())
		}

		contents = append(contents, content)
	}

	return contents
}

func (t *Table) Reset() {
	panic("implement me")
}
