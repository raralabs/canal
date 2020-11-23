package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)
type Bird struct {
	Block_expression string `json = "block_expression"`
	Config 			 string
}
func main() {
	var bird Bird
	file,err := os.Open("C:\\Users\\Subodh\\go\\src\\canal\\examples\\json.reader\\ledger config.txt")

	if err!=nil{
		fmt.Println("error in reading the json file")
	}
	byteValue, _ := ioutil.ReadAll(file)
	json.Unmarshal(byteValue,bird)
	//mapData := jsonParsed.Data().(map[string]interface{})
	//for key,value := range mapData{
	//	fmt.Println(key,value)
	//}
	fmt.Println(bird)
}

