package table

//
//import (
//	"github.com/raralabs/canal/core/message"
//	"log"
//	"sync"
//)
//
//// A MemTable is a table that is stored in the RAM
//type MemTable struct {
//	tableMu *sync.Mutex
//	table   map[string][]message.MsgFieldValue // The table that holds the data
//	fields  []string                            // The fields of the table in order
//	ctx     Context                             // Context of the table
//	alias   string
//}
//
//// NewTable creates a new table with the provided fields and defaultSize for
//// each field.
//func NewMemTable(name string, ctx Context, fields ...string) *MemTable {
//	var flds []string
//	var table map[string][]message.MsgFieldValue
//
//	if len(fields) > 0 {
//		table = make(map[string][]message.MsgFieldValue)
//		flds = make([]string, len(fields))
//
//		for i, fld := range fields {
//			flds[i] = fld
//		}
//	}
//
//	return &MemTable{tableMu: &sync.Mutex{}, table: table, ctx: ctx, fields: flds, alias: name}
//}
//
//func (tbl *MemTable) Alias() string {
//	return tbl.alias
//}
//
//// Insertable cheks if the given message value is insertable to the
//// table on the basis of it's context
//func (tbl *MemTable) Insertable(msg message.MsgContent) bool {
//	return tbl.ctx.Insertable(tbl, msg)
//}
//
//// Insert inserts the message value to the table.
//func (tbl *MemTable) Insert(msg message.MsgContent) {
//	tbl.ctx.Insert(tbl, msg)
//}
//
//// AddFields adds a column to the table. This function works only if the
//// length of the table is 0 i.e. table.Len() == 0
//func (tbl *MemTable) AddFields(fields ...string) {
//	if len(fields) != 0 && tbl.Len() == 0 {
//		for _, f1 := range fields {
//			in := false
//			for _, f2 := range tbl.fields {
//				if f1 == f2 {
//					in = true
//					break
//				}
//			}
//			if !in {
//				tbl.fields = append(tbl.fields, f1)
//			}
//		}
//
//		tbl.tableMu.Lock()
//		if tbl.table == nil {
//			tbl.table = make(map[string][]message.MsgFieldValue)
//		}
//		tbl.tableMu.Unlock()
//	}
//}
//
//// Returns the fields of the table.
//func (tbl *MemTable) Fields() []string {
//	flds := make([]string, len(tbl.fields))
//	copy(flds, tbl.fields)
//	return flds
//}
//
//// Append appends a row of values to the table.
//func (tbl *MemTable) Append(vals []message.MsgFieldValue) {
//
//	if len(vals) != len(tbl.fields) {
//		log.Fatalln("Can only append if the length of values and fields match")
//		return
//	}
//
//	tbl.tableMu.Lock()
//
//	for i, v := range tbl.fields {
//		tbl.table[v] = append(tbl.table[v], vals[i])
//	}
//
//	tbl.tableMu.Unlock()
//}
//
//// Len returns the number of rows in the table.
//func (tbl *MemTable) Len() int {
//
//	tbl.tableMu.Lock()
//	defer tbl.tableMu.Unlock()
//
//	if len(tbl.fields) == 0 {
//		return 0
//	}
//	depth := 0
//	if v, ok := tbl.table[tbl.fields[0]]; ok {
//		depth = len(v)
//	}
//
//	return depth
//}
//
//// RemoveRow removes a row from the table
//func (tbl *MemTable) RemoveRow(row int) {
//
//	tbl.tableMu.Lock()
//	defer tbl.tableMu.Unlock()
//
//	if len(tbl.fields) == 0 {
//		return
//	}
//	depth := len(tbl.table[tbl.fields[0]])
//
//	if row >= depth {
//		return
//	}
//
//	for k := range tbl.table {
//		tbl.table[k][row] = tbl.table[k][len(tbl.table[k])-1]
//		tbl.table[k] = tbl.table[k][:len(tbl.table[k])-1]
//	}
//}
//
//// Iterate through the rows of the table. The values are in the same order as
//// the fields of the table.
//func (tbl *MemTable) IterRows() <-chan []*message.MsgFieldValue {
//
//	tbl.tableMu.Lock()
//	defer tbl.tableMu.Unlock()
//
//	if len(tbl.fields) == 0 {
//		ch := make(chan []*message.MsgFieldValue)
//		close(ch)
//		return ch
//	}
//	depth := len(tbl.table[tbl.fields[0]])
//
//	if depth == 0 {
//		ch := make(chan []*message.MsgFieldValue)
//		close(ch)
//		return ch
//	}
//
//	ch := make(chan []*message.MsgFieldValue, depth)
//
//	go func() {
//		for i := 0; i < depth; i++ {
//			row := make([]*message.MsgFieldValue, len(tbl.fields))
//			tbl.tableMu.Lock()
//			for j, v := range tbl.fields {
//				row[j] = tbl.table[v][i]
//			}
//			tbl.tableMu.Unlock()
//			ch <- row
//		}
//		close(ch)
//	}()
//
//	return ch
//}
