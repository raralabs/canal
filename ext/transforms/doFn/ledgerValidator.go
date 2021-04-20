package doFn

import (
	"fmt"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/message/content"
	"github.com/raralabs/canal/core/pipeline"
	"github.com/raralabs/canal/core/transforms/do"
	"github.com/raralabs/canal/utils/regparser"
	"regexp"
)

type validation struct {
	name           string
	validationRule string
	preValidation  bool
	ValReq         bool
	ValType        content.FieldValueType
}

//{"display_name":"Trace ID","name":"trace_id","validation":{"pre_validation":"str(int(trace_id)) if trace_id else trace_id","required":false,"type":"string"}},

type reconValidator struct {
	fields map[string]validation
}

func NewReconValidator() *reconValidator {
	var recValidator reconValidator
	keys := []string{"card_no", "transaction_date", "account_no",
		"ft_id", "auth_no", "terminal_id", "opening_balance",
		"closing_balance", "account_name", "status", "trace_id"}
	fields := make(map[string]validation)
	recValidator.fields = fields
	for _, key := range keys {
		switch key {
		case "card_no":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "transaction_date":
			var field validation
			field.name = key
			field.ValReq = true
			field.validationRule = `20\d{2}\s?[-]\d{1,2}\s?[-]\d{1,2}`
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "account_no":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "ft_id":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "auth_no":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "terminal_id":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "transaction_amount":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.FLOAT
			recValidator.fields[key] = field
		case "opening_balance":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.FLOAT
			recValidator.fields[key] = field
		case "closing_balance":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.FLOAT
			recValidator.fields[key] = field
		case "account_name":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "status":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			recValidator.fields[key] = field
		case "trace_id":
			var field validation
			field.name = key
			field.ValReq = false
			field.ValType = content.STRING
			field.preValidation = true
			recValidator.fields[key] = field
		default:
			break
		}

	}
	return &recValidator
}
func (rv *reconValidator) ReconFieldValidator(keys ...string) pipeline.Executor {
	df := func(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
		msg := m.Content().Copy()
		if v, ok := msg.Get("eof"); ok {
			if v.Val == true {
				proc.Result(m, msg, nil)
				return true
			}
		}
		for _, key := range keys {
			if v, ok := msg.Get(key); ok {
				//if v.ValType!=rv.fields[key].ValType{
				//	panic ("value mismatch of {#keys}")
				//}
				if rv.fields[key].ValReq == true {
					reg, err := regexp.Compile(rv.fields[key].validationRule)
					if err != nil {
						panic("could not compile regex")
					}
					valType := v.ValType
					if valType == content.INT {
						ok := regparser.ValidateData(reg, fmt.Sprint(v.Val.(int)))
						if ok {
							proc.Result(m, msg, nil)
						}
					} else if valType == content.STRING {
						ok := regparser.ValidateData(reg, v.Val.(string))
						if ok {
							proc.Result(m, msg, nil)
						}
					}
				}
			}
		}
		if v, ok := msg.Get("eof"); ok {
			if v.Val == true {
				proc.Result(m, msg, nil)
				return true
			}
		}
		return true
	}
	return do.NewOperator(df)
}
