package agg

import (
	"sync"

	"github.com/raralabs/canal/core/message"
)

type Table struct {
	tblMu      *sync.Mutex                         // The mutex that synchronizes read and write of table
	aggs       []IAggregator                       // The aggregator functions
	table      map[string][]*message.MsgFieldValue // The table that holds all the aggregator info
	groupField []string                            // The fields to be grouped by
	noGroup    *message.OrderedContent             // Holds Data if grouping is not used
}

// NewTable creates a new aggregator table that can either group the messages
// or not.
func NewTable(aggs []IAggregator, groupBy ...string) *Table {

	var table map[string][]*message.MsgFieldValue
	var groups []string
	var nGrp *message.OrderedContent

	if len(groupBy) != 0 {
		table = make(map[string][]*message.MsgFieldValue)
		groups = make([]string, len(groupBy))
		for i, s := range groupBy {
			groups[i] = s
		}
	} else {
		nGrp = message.NewOrderedContent()
		for _, agg := range aggs {
			nGrp.Add(agg.Name(), agg.InitValue())
		}
	}

	return &Table{aggs: aggs, table: table,
		groupField: groups, noGroup: nGrp,
		tblMu: &sync.Mutex{}}
}

// Insert inserts a message to the Table and updates the either the
// grouping table or noGroup map
func (tbl *Table) Insert(msg *message.OrderedContent) {

	tbl.tblMu.Lock()
	defer tbl.tblMu.Unlock()

	// No Grouping Used
	if len(tbl.groupField) == 0 {
		for _, agg := range tbl.aggs {
			currVal, _ := tbl.noGroup.Get(agg.Name())
			updateVal := agg.Aggregate(currVal, msg)
			tbl.noGroup.Add(agg.Name(), updateVal)
		}
		return
	}

	// Extract data from the required fields
	data := message.NewOrderedContent()
	mvals := *msg
	for _, grp := range tbl.groupField {
		if v, ok := mvals.Get(grp); ok {
			data.Add(grp, v)
		} else {
			return
		}
	}

	depth := len(tbl.table[tbl.groupField[0]])

	for i := 0; i < depth; i++ {
		out := true
		for _, v := range tbl.groupField {
			val, _ := data.Get(v)
			x := val.Value() == tbl.table[v][i].Value()
			out = out && x
		}

		if out {
			// fmt.Println("// Group has matched")
			for _, agg := range tbl.aggs {
				currVal := tbl.table[agg.Name()][i]
				updateVal := agg.Aggregate(currVal, msg)
				tbl.table[agg.Name()][i] = updateVal
			}

			return
		}
	}

	// If no match to any existing values, insert the values in the table
	for _, v := range tbl.groupField {
		val, _ := data.Get(v)
		tbl.table[v] = append(tbl.table[v], val)
	}

	for _, agg := range tbl.aggs {
		tbl.table[agg.Name()] = append(tbl.table[agg.Name()], agg.InitMsgValue(msg))
	}
}

// Messages creates an array of messages for each of the rows in the table and
// returns it.
func (tbl *Table) Messages() []*message.OrderedContent {

	tbl.tblMu.Lock()
	defer tbl.tblMu.Unlock()

	// If there was no grouping, just a single message is generated.
	if len(tbl.groupField) == 0 {
		msg := message.NewOrderedContent()
		for e := tbl.noGroup.First(); e != nil; e = e.Next() {
			k, _ := e.Value.(string)
			v, _ := tbl.noGroup.Get(k)
			msg.Add(k, v)
		}

		return []*message.OrderedContent{msg}
	}

	if len(tbl.table) == 0 {
		// No Messages have been inserted
		return []*message.OrderedContent{}
	}

	if grp, ok := tbl.table[tbl.groupField[0]]; ok {
		depth := len(grp)
		msgs := make([]*message.OrderedContent, depth)

		for i := 0; i < depth; i++ {
			msg := message.NewOrderedContent()

			// Collect Groups data first
			for _, k := range tbl.groupField {
				msg.Add(k, tbl.table[k][i])
			}

			// Then, collect aggregator's data
			for _, ag := range tbl.aggs {
				k := ag.Name()
				msg.Add(k, tbl.table[k][i])
			}

			msgs[i] = msg
		}

		return msgs
	}

	return []*message.OrderedContent{}
}

// Reset resets the table
func (tbl *Table) Reset() {
	tbl.tblMu.Lock()
	defer tbl.tblMu.Unlock()

	for k := range tbl.table {
		delete(tbl.table, k)
	}

	// for k := range tbl.noGroup {
	// 	delete(tbl.noGroup, k)
	// }

	for _, ag := range tbl.aggs {
		ag.Reset()
	}
}
