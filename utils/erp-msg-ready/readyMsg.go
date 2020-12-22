package erp_msg_ready

import (
	"encoding/json"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/pg-service-client/handlers/models"
	"log"
	"strings"
)

type NewPgMsg struct{
	subject string
	msg        message.Msg
}


func NewErpMsg (subject string,msg message.Msg)*NewPgMsg{
	var subjectArr []string
	subjectArr = strings.Split(subject, ".")
	return &NewPgMsg{subject: subjectArr[1],msg:msg}
}

func(pgMsg *NewPgMsg)MsgSelector()[]byte{
	switch pgMsg.subject{
	case "write":
		var content *models.WriteRequestContent
		RequestID:= "1"
		AuthIdentity := ""
		content = &models.WriteRequestContent{Namespace:"tigg",
			Collection:"TestTable",
			RequestedBy: "canal",
			SchemaID: int64(1),
			Partition: int64(0),
			Data:pgMsg.msg.Values(),
		}
		bytearr,err := json.Marshal(&models.WriteRequest{RequestID: RequestID,SourceData: nil,AuthIdentity: AuthIdentity,Content: content})
		if err!=nil{
			log.Panic(err)
		}
		return bytearr
	case "setup":
		var content *models.InitialSetupRequestContent
		RequestID:= "1"
		AuthIdentity := ""
		content = &models.InitialSetupRequestContent{}
		bytearr,err := json.Marshal(&models.InitialSetupRequest{RequestID: RequestID,AuthIdentity: AuthIdentity,Content: *content})
		if err!=nil{
			log.Panic(err)
		}
		return bytearr
	}

	return []byte("hello")
}