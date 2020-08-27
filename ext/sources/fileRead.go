package sources

import (
	"bufio"
	content2 "github.com/raralabs/canal/core/message/content"
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
		content := content2.New()
		line := fr.scanner.Text()
		content = content.Add(fr.key, content2.NewFieldValue(line, content2.STRING))
		proc.Result(m, content, nil)
	} else {
		content := content2.New()
		content = content.Add("eof", content2.NewFieldValue(true, content2.BOOL))
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
