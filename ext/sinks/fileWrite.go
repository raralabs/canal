package sinks

import (
	"fmt"
	"log"
	"os"

	"github.com/raralabs/canal/core/message"
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

	//w := bufio.NewWriter(f)
	return &FileWriter{name: "FileWriter", writer: f, key: key}
}

func (fw *FileWriter) ExecutorType() pipeline.ExecutorType {
	return pipeline.SINK
}

func (fw *FileWriter) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {

	content := m.Content()
	if val, ok := content.Get(fw.key); ok {
		str := fmt.Sprintf("%v\n", val.Val)
		fw.writer.WriteString(str)
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
