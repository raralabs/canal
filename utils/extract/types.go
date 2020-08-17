package extract

import (
	"github.com/raralabs/canal/core/message/content"
	"strconv"
)

func ValType(v interface{}) (interface{}, content.FieldValueType) {

	if v == nil {
		return nil, content.NONE
	}

	switch val := v.(type) {
	case bool:
		return val, content.BOOL

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return val, content.INT

	case float32, float64:
		return val, content.FLOAT

	case string:
		if value, err := strconv.ParseInt(val, 10, 64); err == nil {
			return value, content.INT
		} else if value, err := strconv.ParseFloat(val, 64); err == nil {
			return value, content.FLOAT
		}

		return val, content.STRING
	}

	return v, content.NONE
}
