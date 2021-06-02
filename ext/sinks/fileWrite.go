package sinks

import (
	"fmt"
	"log"
	"os"

	//"github.com/raralabs/canal/core/message"//import for older version
	"github.com/raralabs/canal/core/pipeline"
)

type FileWriter struct {
	name   string
	writer *os.File
	key    string
}

func NewFileWriter(path, key string) pipeline.Executor {
	// Create a file for writing
	f, err := os.Create(path)
	if err != nil {
		log.Panicf("Couldn't open file for writing. Error: %v", err)
	}

	return &FileWriter{name: "FileWriter", writer: f, key: key}
}

func (fw *FileWriter) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

func (fw *FileWriter) Execute(m pipeline.MsgPod, proc pipeline.IProcessorForExecutor) bool {

	if m.Msg.Eof() {
		proc.Done()
		return false
	}

	content := m.Msg.Content()
	if val, ok := content.Get(fw.key); ok {
		str := fmt.Sprintf("%v\n", val.Val)
		_, err := fw.writer.WriteString(str)
		if err != nil {
			log.Println("[ERROR]", err)
		}
	}

	return false
}

func (fw *FileWriter) HasLocalState() bool {
	return false
}

func (fw *FileWriter) SetName(name string) {
	fw.name = name
}

func (fw *FileWriter) Name() string {
	return fw.name
}
