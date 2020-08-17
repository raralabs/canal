package message

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/raralabs/canal/core/message/content"
)

// MsgType represents the supported types for message
type MsgType uint8

// These are the currently supported types
const (
	CONTROL MsgType = iota + 1 // CONTROL message tells the processors to persist the current state in the disk
	ERROR                      // ERROR message is used for reporting errors in the pipeline
	EXECUTE                    // EXECUTE type messages are for the messages that are unbound (indefinitely streaming)
)

type Msg struct {
	id             uint64           // id of the Msg
	pipelineId     uint32           // id of the pipeline from which the message was created
	stageId        uint32           // id of the latest stage through which the message has passed
	processorId    uint32           // id of the latest processor which has generated the message
	srcStageId     uint32           //
	srcProcessorId uint32           //
	srcMessageId   uint64           //
	mtype          MsgType          // MsgType of the message
	mcontent       content.IContent // MsgContent of the message
	prevContent    content.IContent // Content of previous message
	trace          trace            // trace of the message
}

func NewError(pipelineId uint32, stageId uint32, processorId uint32, code uint8, text string) Msg {

	contents := content.New()
	contents.Add("text", content.NewFieldValue(text, content.STRING))
	contents.Add("code", content.NewFieldValue(code, content.INT))

	return Msg{
		pipelineId:  pipelineId,
		stageId:     stageId,
		processorId: processorId,
		mcontent:    contents,
	}
}

// NewFromBytes creates a new message on the basis of the byte array 'bts'.
// The byte array MUST be gob-encoded.
func NewFromBytes(bts []byte) (*Msg, error) {
	var m Msg
	var buf bytes.Buffer
	buf.Write(bts)
	decoder := gob.NewDecoder(&buf)
	err := decoder.Decode(&m)
	if err != nil {
		return nil, err
	}

	return &m, err
}

func (m *Msg) Id() uint64 {
	return m.id
}

// SetField adds a (key, value) pair to the data stored by the Msg and
// returns it.
func (m *Msg) SetField(key string, value *content.MsgFieldValue) *Msg {
	m.mcontent.Add(key, value)
	return m
}

// MsgContent returns the data stored by the message.
func (m *Msg) Content() content.IContent {
	return m.mcontent
}

// MsgContent returns the data stored by the message.
func (m *Msg) PrevContent() content.IContent {
	return m.prevContent
}

// MsgContent returns the data stored by the message.
func (m *Msg) SetPrevContent(content content.IContent) {
	m.prevContent = content
}

// Values returns a map with just keys and values in the message, without type information.
//? Caching the map might lead to better performance
func (m *Msg) Values() map[string]interface{} {
	return m.mcontent.Values()
}

func (m *Msg) Trace() *trace {
	return &m.trace
}

// Types returns a map with just keys and values types in the message, without
// actual values.
//? Caching the map might lead to better performance
func (m *Msg) Types() map[string]content.FieldValueType {
	return m.mcontent.Types()
}

// AsBytes returns the gob-encoded byte array of the message.
func (m *Msg) AsBytes() ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(*m)

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *Msg) StageId() uint32 {
	return m.stageId
}

func (m *Msg) ProcessorId() uint32 {
	return m.processorId
}

func (m *Msg) String() string {
	return fmt.Sprintf(
		"Msg[Id:%d, Stg:%d, Prc:%d; %s]",
		m.id, m.stageId, m.processorId, m.mcontent.String())
}

func (m *Msg) IsControl() bool {
	return m.mtype == CONTROL
}

func (m *Msg) IsError() bool {
	return m.mtype == ERROR
}

func (m *Msg) IsExecute() bool {
	return m.mtype == EXECUTE
}
