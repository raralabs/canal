package sources

import (
	"bufio"
	"log"
	"os"

	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/pipeline"
)

type FileReader struct {
	name     string
	scanner  *bufio.Scanner
	maxLines int
	key      string
	buf      *os.File
}

func NewFileReader(path, key string, maxLines int) pipeline.Executor {
	buf, err := os.Open(path)
	if err != nil {
		log.Panic(err)
	}

	if key == "eof" {
		log.Panic("Key can't be eof")
	}

	scn := bufio.NewScanner(buf)
	return &FileReader{name: "FileReader",
		key:      key,
		buf:      buf,
		scanner:  scn,
		maxLines: maxLines,
	}
}

func (fr *FileReader) Execute(m message.Msg, proc pipeline.IProcessorForExecutor) bool {
	if fr.maxLines == 0 {
		proc.Done()
		fr.close()
		return false
	}

	if fr.maxLines > 0 {
		fr.maxLines--
	}

	if fr.scanner.Scan() {
		content := message.NewOrderedContent()
		line := fr.scanner.Text()
		content.Add(fr.key, message.NewFieldValue(line, message.STRING))
		proc.Result(m, content, nil)
	} else {
		content := message.NewOrderedContent()
		content.Add("eof", message.NewFieldValue(true, message.BOOL))
		proc.Result(m, content, nil)

		proc.Done()
		fr.close()
	}

	return false
}

func (fr *FileReader) ExecutorType() pipeline.ExecutorType {
	return pipeline.SOURCE
}

func (fr *FileReader) HasLocalState() bool {
	return false
}

func (fr *FileReader) SetName(name string) {
	fr.name = name
}

func (fr *FileReader) Name() string {
	return fr.name
}

func (fr *FileReader) close() {
	if err := fr.buf.Close(); err != nil {
		log.Panic(err)
	}
}
