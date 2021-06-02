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
	CONTROL MsgType = iota + 1 // CONTROL message tells the processors to persist the
	// current state in the disk

	ERROR // ERROR message is used for reporting errors in the pipeline

	EXECUTE // EXECUTE type messages are for the messages that are unbound
	// (indefinitely streaming)
)

type Msg struct {
	id             uint64           // id of the Msg
	pipelineId     uint32           // id of the pipeline from which the message was created
	stageId        uint32           // id of the latest stage through which the message has passed
	processorId    uint32           // id of the latest processor which has generated the message
	srcStageId     uint32           //
	srcProcessorId uint32           //
	srcMessageId   uint64           //
	msgType        MsgType          // MsgType of the message
	msgContent     content.IContent // msgContent of the message
	prevContent    content.IContent // Content of previous message
	trace          trace            // trace of the message
	eof            bool             // flag to check if the message is the last one
}

func (m *Msg) Id() uint64 {
	return m.id
}

// SetField adds a (key, value) pair to the data stored by the Msg and
// returns it.
func (m *Msg) SetField(key string, value content.MsgFieldValue) *Msg {
	m.msgContent = m.msgContent.Add(key, value)
	return m
}

// Content returns the data stored by the message.
func (m *Msg) Content() content.IContent {
	return m.msgContent
}

// PrevContent returns the previous data stored by the message.
func (m *Msg) PrevContent() content.IContent {
	return m.prevContent
}

// SetContent sets content given as the new content of the message.
func (m *Msg) SetContent(content content.IContent) {
	m.msgContent = content
}

// SetPrevContent sets content of time (t) as previous content for (t+1) message.
func (m *Msg) SetPrevContent(content content.IContent) {
	m.prevContent = content
}

// Values returns a map with just keys and values in the message, without type information.
//? Caching the map might lead to better performance
func (m *Msg) Values() map[string]interface{} {
	return m.msgContent.Values()
}

func (m *Msg) Trace() *trace {
	return &m.trace
}

// Types returns a map with just keys and values types in the message, without
// actual values.
//? Caching the map might lead to better performance
func (m *Msg) Types() map[string]content.FieldValueType {
	return m.msgContent.Types()
}

func (m *Msg) StageId() uint32 {
	return m.stageId
}

func (m *Msg) ProcessorId() uint32 {
	return m.processorId
}

func (m *Msg) PipelineId() uint32 {
	return m.pipelineId
}

func (m *Msg) String() string {
	return fmt.Sprintf(
		"Msg[Id:%d, Stg:%d, Prc:%d,Content:%s]",
		m.id, m.stageId, m.processorId, m.msgContent)
}

func (m *Msg) IsControl() bool {
	return m.msgType == CONTROL
}

func (m *Msg) IsError() bool {
	return m.msgType == ERROR
}

func (m *Msg) IsExecute() bool {
	return m.msgType == EXECUTE
}

func (m *Msg) Eof() bool {
	return m.eof
}

func (m *Msg) SetEof(b bool) {
	m.eof = b
}

// msgHolder holds the message to be serialized.
type msgHolder struct {
	Id                   uint64
	PipelineId           uint32
	StageId              uint32
	ProcessorId          uint32
	SrcStageId           uint32
	SrcProcessorId       uint32
	SrcMessageId         uint64
	Mtype                MsgType
	McontentValues       map[string]interface{}
	PrevContentValues    map[string]interface{}
	McontentType         map[string]content.FieldValueType
	PrevContentType      map[string]content.FieldValueType
	NilFlagForContent    bool //flag to check if the message content is nil inorder to prevent encoding errors
	NilFlagForPreContent bool //flag to check if the previous content is nil inorder to prevent encoding errors
	TraceFlag            bool
	TracePath            []tracePath
}

// AsBytes returns the gob-encoded byte array of the message.
func (m *Msg) AsBytes() ([]byte, error) {
	var buf bytes.Buffer

	MessageHolder := &msgHolder{
		Id:             m.id,
		PipelineId:     m.pipelineId,
		StageId:        m.stageId,
		ProcessorId:    m.processorId,
		SrcStageId:     m.srcStageId,
		SrcProcessorId: m.srcProcessorId,
		SrcMessageId:   m.srcMessageId,
		Mtype:          m.msgType,
		TraceFlag:      m.trace.enabled,
		TracePath:      m.trace.path,
	}
	if m.msgContent != nil {
		MessageHolder.McontentValues = m.msgContent.Values()
		MessageHolder.McontentType = m.msgContent.Types()
	} else {
		nilValues := make(map[string]interface{})
		nilValues["key"] = "nil"
		MessageHolder.McontentValues = nilValues
		MessageHolder.NilFlagForContent = true
	}
	if m.prevContent != nil {
		MessageHolder.PrevContentValues = m.prevContent.Values()
		MessageHolder.PrevContentType = m.prevContent.Types()
	} else {
		nilValues := make(map[string]interface{})
		nilValues["key"] = "nil"
		MessageHolder.PrevContentValues = nilValues
		MessageHolder.NilFlagForPreContent = true
	}
	encoder := gob.NewEncoder(&buf)
	err := encoder.Encode(MessageHolder)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// NewFromBytes creates a new message on the basis of the byte array 'bts'.
// The byte array MUST be gob-encoded.
func NewFromBytes(bts []byte) (*Msg, error) {
	var m *msgHolder
	var buf bytes.Buffer
	buf.Write(bts)
	err := gob.NewDecoder(&buf).Decode(&m)
	if err != nil {
		return nil, err
	}

	currentDecodedMsgContent := content.New()
	prevDecodedMsgContent := content.New()

	if !m.NilFlagForContent {
		for key, value := range m.McontentValues {
			currentDecodedMsgContent.Add(key, content.NewFieldValue(value, m.McontentType[key]))
		}
	} else {
		currentDecodedMsgContent = nil
	}

	if !m.NilFlagForPreContent {
		for key, value := range m.PrevContentValues {
			prevDecodedMsgContent.Add(key, content.NewFieldValue(value, m.PrevContentType[key]))
		}
	} else {
		prevDecodedMsgContent = nil
	}

	message := &Msg{
		id:             m.Id,
		pipelineId:     m.PipelineId,
		stageId:        m.StageId,
		processorId:    m.ProcessorId,
		srcStageId:     m.SrcStageId,
		srcProcessorId: m.SrcProcessorId,
		srcMessageId:   m.SrcMessageId,
		msgType:        m.Mtype,
		msgContent:     currentDecodedMsgContent,
		prevContent:    prevDecodedMsgContent,
		trace:          trace{enabled: m.TraceFlag, path: m.TracePath},
	}
	return message, nil
}

func NewError(pipelineId uint32, stageId uint32, processorId uint32, code uint8, text string) Msg {
	cont := content.New()
	cont = cont.Add("text", content.NewFieldValue(text, content.STRING))
	cont = cont.Add("code", content.NewFieldValue(code, content.INT))
	return Msg{
		pipelineId:  pipelineId,
		stageId:     stageId,
		processorId: processorId,
		msgContent:  cont,
	}
}

func NewEof(pipelineId uint32, stageId uint32, processorId uint32) Msg {
	return Msg{
		pipelineId:  pipelineId,
		stageId:     stageId,
		processorId: processorId,
		msgContent:  nil,
		eof:         true,
	}
}
