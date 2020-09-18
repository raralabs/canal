package joinUsingHshMap

import (
	"github.com/raralabs/canal/core/message/content"
	"math/rand"
	"time"
)

var dummyMsg = map[string][]interface{}{
	"first_name":{"Ram","Hari","geeta","sita"},
	"last_name":{"shakya","shrestha","Rai","Tamang"},
	"age":{21,26,25,24},
	"full_name":{"Ram karki","Hari Tamang","Ram Shakya","Ram Rai","Ram Shrestha","Hari Shrestha","geeta Rai","sita Tamang"},
	"weight":{55,65,64,54,30,32,45,52},
}


func createMsgForPath1(loopTill int,messageChannel chan<-content.IContent){
	rand.Seed(time.Now().UnixNano())

	var fields = []string{"first_name","last_name","age"}
	for i:=0;i<=loopTill;i++{
		msgContent := content.New()
		for _,field := range fields{
			random := rand.Intn(len(dummyMsg[field]))
			if field == "age" || field=="weight"{
				msgContent.Add(field,content.NewFieldValue(dummyMsg[field][random],content.INT))
			}else{
				msgContent.Add(field,content.NewFieldValue(dummyMsg[field][random],content.STRING))
			}
		}
		messageChannel<- msgContent
	}
	close(messageChannel)
}

func createMsgForPath2(loopTill int,messageChannel chan<-content.IContent){
	rand.Seed(time.Now().UnixNano())
	var fields = []string{"full_name","age","weight"}
	for i:=0;i<=loopTill;i++{
		msgContent := content.New()
		for _,field := range fields{
			random := rand.Intn(len(dummyMsg[field]))
			if field == "age" || field =="weight"{
				msgContent.Add(field,content.NewFieldValue(dummyMsg[field][random],content.INT))
			}else{
				msgContent.Add(field,content.NewFieldValue(dummyMsg[field][random],content.STRING))
			}
		}
		messageChannel<- msgContent
	}
	close(messageChannel)
}



