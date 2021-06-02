package cast

import (
	"fmt"
	"strconv"
)

// TryBool tries to convert the given interface to bool and returns it if
// it is successful. An additional boolean flag denotes if it was successful
// or not.
func TryBool(v interface{}) (bool, bool) {
	switch val := v.(type) {
	case bool:
		return val, true
	case string:
		b, err := strconv.ParseBool(val)
		if err == nil {
			return b, true
		}
	}

	return false, false
}

// TryInt tries to convert the given interface to int64 and returns it if
// it is successful. An additional boolean flag denotes if it was successful
// or not.
func TryInt(v interface{}) (int64, bool) {
	switch val := v.(type) {
	case int:
		return int64(val), true
	case int8:
		return int64(val), true
	case int16:
		return int64(val), true
	case int32:
		return int64(val), true
	case int64:
		return val, true
	case uint:
		return int64(val), true
	case uint16:
		return int64(val), true
	case uint32:
		return int64(val), true
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, true
		}
	}

	return 0, false
}

// TryFloat tries to convert the given interface to float64 and returns it if
// it is successful. An additional boolean flag denotes if it was successful
// or not.
func TryFloat(v interface{}) (float64, bool) {
	switch val := v.(type) {
	case int:
		return float64(val), true
	case int8:
		return float64(val), true
	case int16:
		return float64(val), true
	case int32:
		return float64(val), true
	case int64:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint16:
		return float64(val), true
	case uint32:
		return float64(val), true
	case uint64:
		return float64(val), true
	case float32:
		return float64(val), true
	case float64:
		return val, true
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f, true
		}
	}

	return 0.0, false
}

// ToIfaceSlice converts an interface to slice of interfaces if it is not nil.
func ToIfaceSlice(v interface{}) []interface{} {
	if v == nil {
		return nil
	}

	return v.([]interface{})
}

// TryString tries to convert the given interface to string and returns it if
// it is successful. An additional boolean flag denotes if it was successful
// or not.
func TryString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case []uint8:
		return string(s), true
	case string:
		return s, true
	}

	return "", false
}

// GetStringValue converts the given string to either bool, int, float or
// single quoted string.
func GetStringValue(str string) interface{} {

	var val interface{}
	val = str

	var value interface{}
	if vi, ok := TryInt(val); ok {
		value = vi
	} else if vb, ok := TryBool(val); ok {
		value = vb
	} else if vf, ok := TryFloat(val); ok {
		value = vf
	} else {
		value = fmt.Sprintf("'%s'", str)
	}

	return value
}
