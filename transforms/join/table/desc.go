package table

//
//import (
//	"log"
//	"strings"
//)
//
//// A Description describes the essential stuffs of a table
//type Description struct {
//	Alias  string   // Name of the table
//	Fields []string // FieldNames stored in the table
//	Ctx    Context  // Context of the table
//	Type   string   // ValueType of table. Can be "mem", "disk", and so on.
//}
//
//func NewTable(desc *Description) Table {
//
//	t := strings.ToLower(desc.Type)
//
//	switch t {
//	case "mem":
//		tbl := NewMemTable(desc.Alias, desc.Ctx, desc.Fields...)
//		return tbl
//	default:
//		log.Fatalf("Unsupported type of table: %s", t)
//	}
//
//	return nil
//}
