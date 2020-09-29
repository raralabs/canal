package joinUsingHshMap

import (
	"fmt"
	"github.com/dorin131/go-data-structures/linkedlist"
	"github.com/raralabs/canal/core/message/content"
	"math"
	"strings"
	"sync"
)

const arrayLength = uint64(1000)

type HashTable struct {

	data   [arrayLength]*linkedlist.LinkedList
	iterateLock 	sync.Mutex
}

type listData struct{
	key uint64
	value interface{}
}

//creates a new Hash Table for storing data
func NewHashMap() *HashTable{
	return &HashTable{data: [arrayLength]*linkedlist.LinkedList{}}
}
func concatKeys(keys []interface{})string{
	concatenatedkey := ""
	for _,key := range keys{
		concatenatedkey+=fmt.Sprintf("%v",key)+" "

	}
	return strings.TrimSpace(concatenatedkey)
}

//gives the concatenated key and hash
func createHash(concatKey string)uint64{
	hash := uint64(0)

	for pos,char := range concatKey{

		hash += uint64(char) * uint64(math.Pow(27, float64(len(concatKey)-pos+1)))

		//fmt.Println("hash",string(char),int(math.Pow(31,float64(len(concatenatedkey)-pos+1))))
		}

	return hash
}

//returns the index for the hash to be stored in hash map
func index(hash uint64)uint64{
	return hash % arrayLength
}

//It first calculate the index for key first by hashing
//and then get the index using modulo. If there is nothing
// at the position of index, we create newlinkedlist,otherwise
//we iterate through the list and check whether the node needs
//update or should we add new node
func (hshTable *HashTable) Set(v interface{},concatKey string) *HashTable {
	hash := createHash(concatKey)
	index := index(hash)
	if hshTable.data[index] == nil {
		hshTable.data[index] = linkedlist.New()
		hshTable.data[index].Append(listData{hash, v})
	} else {
		node := hshTable.data[index].Head
		for {
			if node != nil {
				d := node.Data.(listData)
				if d.key == hash {
					d.value = v
					break
				} else {
					hshTable.data[index].Append(listData{hash, v})
					break
				}
			}
		}
	}
	return hshTable
}


//Get method calculates the index and then we look through the linked list
//and look for the required value
//func (hshTable *HashTable)Get(concatKey string) (result interface{},ok bool){
//	hash := createHash(concatKey)
//	index := index(hash)
//	linkedList := hshTable.data[index]
//	if linkedList == nil{
//		return nil,false
//	}
//	node := linkedList.Head
//	for {
//		if node !=nil{
//			d := node.Data.(listData)
//
//			if d.key == hash{
//				return d.value,true
//			}
//		}else{
//			return nil,false
//		}
//		node = node.Next
//	}
//
//}
//modified for the outer join
func (hshTable *HashTable)Get(ConcatKey string)(result interface{},ok bool){

	hash := createHash(ConcatKey)
	index := index(hash)
	linkedList := hshTable.data[index]
	if linkedList == nil{
		return nil,false
	}

	node := linkedList.Head
	for {
		if node !=nil{

			d := node.Data.(listData)
			if d.key == hash{
				for node.Next != nil {
					node.Next = node.Next.Next
				}
				node = node.Next
				return d.value,true
				}
			}else{
				return nil,false
			}
		node = node.Next
	}

}

func (hshTable *HashTable)iterate(messageChannel chan <- content.IContent) bool{
	linkedList := hshTable.data
	for _,table := range(linkedList){
		if table!=nil {
			node :=table.Head
			for node!= nil{
				messageChannel<- node.Data.(listData).value.(content.IContent)
				node = node.Next
			}
		}
	}
	close(messageChannel)
	return true
}