package transforms

//
//import (
//	"reflect"
//	"testing"
//)
//
//func TestGopherLua_All(t *testing.T) {
//	scripts := make(map[string]string)
//
//	input := map[string]interface{}{"Name": "Nischal", "Roll": int64(422)}
//
//	scripts["mapReturn"] = `
//	function GopherLua(m)
//		return m
//	end
//	`
//
//	scripts["nameChange"] = `
//	function GopherLua(m)
//		m["Name"] = "Nepal"
//		return m
//	end
//	`
//
//	scripts["addRoll"] = `
//	function GopherLua(m)
//		m["Roll"] = m["Roll"] + 1
//		return m
//	end
//	`
//
//	tests := []struct {
//		name  string
//		src   string
//		input map[string]interface{}
//		want  map[string]interface{}
//	}{
//		// TODO: add test cases.
//		{"Map Return Only", "mapReturn", input, map[string]interface{}{"Name": "Nischal", "Roll": int64(422)}},
//		{"Name Change", "nameChange", input, map[string]interface{}{"Name": "Nepal", "Roll": int64(422)}},
//		{"add Roll", "addRoll", input, map[string]interface{}{"Name": "Nischal", "Roll": int64(423)}},
//		{"add Roll", "addRoll", map[string]interface{}{"Name": "Nischal", "Roll": int64(-1)},
//			map[string]interface{}{"Name": "Nischal", "Roll": int64(0)}},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			lua := NewGopherLua([]byte(scripts[tt.src]))
//			if got := lua.Run(tt.input); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Run() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
