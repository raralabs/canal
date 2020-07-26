package cast

import (
	"github.com/raralabs/canal/core/message"
	"strconv"
)

func ValType(v interface{}) (interface{}, message.FieldValueType) {
	switch val := v.(type) {
	case bool:
		return val, message.BOOL

	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return val, message.INT

	case float32, float64:
		return val, message.FLOAT

	case string:
		if value, err := strconv.ParseInt(val, 10, 64); err == nil {
			return value, message.INT
		} else if value, err := strconv.ParseFloat(val, 64); err == nil {
			return value, message.FLOAT
		}

		return val, message.STRING
	}

	return v, message.NONE
}
