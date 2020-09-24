package pipeline

import (
	"context"
	"github.com/raralabs/canal/core/message"
	content2 "github.com/raralabs/canal/core/message/content"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type numberGenerator struct {
	name   string
	curVal uint64
	maxVal uint64
}

func newNumberGenerator(maxVal uint64) Executor {
	return &numberGenerator{name: "Inline", curVal: 0, maxVal: maxVal}
}
func (s *numberGenerator) Execute(m MsgPod, proc IProcessorForExecutor) bool {
	if s.curVal >= s.maxVal {
		proc.Done()
		return true
	}

	s.curVal++
	content := content2.New()
	content = content.Add("value", content2.NewFieldValue(s.curVal, content2.INT))
	proc.Result(m.Msg, content, nil)
	return false
}
func (s *numberGenerator) ExecutorType() ExecutorType {
	return SOURCE
}
func (s *numberGenerator) HasLocalState() bool {
	return false
}
func (s *numberGenerator) SetName(name string) {
	s.name = name
}
func (s *numberGenerator) Name() string {
	return s.name
}

type channelSink struct {
	channel chan message.Msg
	name    string
}

func newSink(ch chan message.Msg) Executor {
	return &channelSink{
		name:    "BlackHole",
		channel: ch,
	}
}
func (s *channelSink) ExecutorType() ExecutorType {
	return SINK
}
func (s *channelSink) Execute(m MsgPod, pr IProcessorForExecutor) bool {
	s.channel <- m.Msg
	return true
}
func (s *channelSink) HasLocalState() bool {
	return false
}
func (s *channelSink) SetName(name string) {
	s.name = name
}
func (s *channelSink) Name() string {
	return s.name
}

func TestNewPipeline(t *testing.T) {

	t.Run("Create New Pipeline", func(t *testing.T) {
		pipeLine := NewPipeline(uint32(1))
		stgFactory := newStageFactory(pipeLine)
		assert.Equal(t,pipeLine.id,uint32(1),"Id didn't match")
		assert.Equal(t,false,pipeLine.runLock.Load(),"run lock must be disabled by default")
		assert.Equal(t,"chan message.Msg",reflect.TypeOf(pipeLine.errorReceiver).String(),"error receiver must of type channel")
		if !reflect.DeepEqual(pipeLine.stageFactory, stgFactory) {
			t.Errorf("Want: %v\nGot: %v\n", stgFactory, pipeLine.stageFactory)
		}
	})
}


func TestPipeline_AddSource(t *testing.T) {
	pipeLine := NewPipeline(uint32(1))
	t.Run("add source", func(t *testing.T) {
		stg := pipeLine.AddSource("source stage")
		assert.Equal(t,stg.pipeline.id,pipeLine.id,"pipeline id mismatch suggest stage created on different pipeline")
		assert.Equal(t,SOURCE,stg.executorType,"Executor Type expected to be SOURCE")
		//trying to add transform and sink processor on source stage
		assert.Panics(t, func() {
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(TRANSFORM))
		})
		assert.Panics(t, func() {
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(SINK))
		})
	})
}


func TestPipeline_AddTransform(t *testing.T) {
	pipeLine := NewPipeline(uint32(1))
	t.Run("add source", func(t *testing.T) {
		stg := pipeLine.AddTransform("source stage")
		assert.Equal(t,stg.pipeline.id,pipeLine.id,"pipeline id mismatch suggest stage created on different pipeline")
		assert.Equal(t,TRANSFORM,stg.executorType,"Executor Type expected to be TRANSFORM")
		// trying to add source and sink processor on transform stage
		assert.Panics(t, func() {
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(SOURCE))
		})
		assert.Panics(t, func() {
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(SINK))
		})
	})
}

func TestPipeline_AddSink(t *testing.T) {
	pipeLine := NewPipeline(uint32(1))
	t.Run("add source", func(t *testing.T) {
		stg := pipeLine.AddSink("source stage")
		assert.Equal(t,stg.pipeline.id,pipeLine.id,"pipeline id mismatch suggest stage created on different pipeline")
		assert.Equal(t,SINK,stg.executorType,"Executor Type expected to be TRANSFORM")
		// trying to add source and transform processor on Sink stage
		assert.Panics(t, func() {
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(SOURCE))
		})
		assert.Panics(t, func() {
			stg.AddProcessor(DefaultProcessorOptions,newDummyExecutor(TRANSFORM))
		})
	})
}

//func TestPipeline_Validate(t *testing.T) {
//	pipeLine := NewPipeline(uint32(1))
//	srcStg := pipeLine.AddSource("Generator")
//	trnStg := pipeLine.AddTransform("Filter")
//	snkStg := pipeLine.AddSink("BlackHole")
//	src := srcStg.AddProcessor(DefaultProcessorOptions, newNumberGenerator(100))
//	trnStg.ReceiveFrom("path1", src)
//	tr := trnStg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM), "path1")
//	snkCh := make(chan message.Msg, 100)
//	snkStg.ReceiveFrom("path2", tr)
//	snkStg.AddProcessor(DefaultProcessorOptions, newSink(snkCh), "")
//	pipeLine.Validate()
//
//}

func TestPipeline_Id(t *testing.T) {
	pipelineId := uint32(1)
	pipeLine := NewPipeline(pipelineId)
	assert.Equal(t,pipelineId,pipeLine.Id(),"Id must match")
}

func TestPipeline(t *testing.T) {

	pipelineId := uint32(1)

	pipeline := NewPipeline(pipelineId)
	assert.Equal(t, pipelineId, pipeline.Id())

	srcStg := pipeline.AddSource("Generator")
	trnStg := pipeline.AddTransform("Filter")
	snkStg := pipeline.AddSink("BlackHole")

	assert.Equal(t, uint32(1), srcStg.GetId())

	src := srcStg.AddProcessor(DefaultProcessorOptions, newNumberGenerator(100))

	trnStg.ReceiveFrom("path1", src)
	tr := trnStg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM), "path1")

	snkCh := make(chan message.Msg, 100)
	snkStg.ReceiveFrom("path2", tr)
	snkStg.AddProcessor(DefaultProcessorOptions, newSink(snkCh), "")

	pipeline.Validate()

	

	bckGnd := context.Background()
	d := time.Now().Add(10 * time.Millisecond)
	ctx, cancel := context.WithDeadline(bckGnd, d)

	go func() {
		var receivedMsgs []interface{}

		for {
			rcvd, ok := <-snkCh
			if !ok {
				break
			}
			if val, ok := rcvd.Content().Get("value"); ok {
				receivedMsgs = append(receivedMsgs, val.Val)
			}
		}

		for i := range receivedMsgs {
			assert.Contains(t, receivedMsgs, uint64(i+1))
		}
	}()

	testAfterCompletion := func() {
		close(snkCh)
	}

	pipeline.Start(ctx, testAfterCompletion)
	for _,stg := range(pipeline.stages){
		assert.Equal(t,true,stg.isRunning(),"start should run the stages")
		assert.Equal(t,true,stg.isClosed(),"start sould run the stages")
	}

	cancel()
}

func BenchmarkPipeline(b *testing.B) {
	b.ReportAllocs()

	pipelineId := uint32(1)

	pipeline := NewPipeline(pipelineId)

	srcStg := pipeline.AddSource("Generator")
	trnStg := pipeline.AddTransform("Filter")
	snkStg := pipeline.AddSink("BlackHole")

	src := srcStg.AddProcessor(DefaultProcessorOptions, newNumberGenerator(100))

	trnStg.ReceiveFrom("path1", src)
	tr := trnStg.AddProcessor(DefaultProcessorOptions, newDummyExecutor(TRANSFORM), "path1")

	snkCh := make(chan message.Msg, 100)
	snkStg.ReceiveFrom("path2", tr)
	snkStg.AddProcessor(DefaultProcessorOptions, newSink(snkCh), "")

	pipeline.Validate()

	bckGnd := context.Background()
	d := time.Now().Add(10 * time.Millisecond)
	ctx, cancel := context.WithDeadline(bckGnd, d)

	for i := 0; i < b.N; i++ {
		go func() {
			for {
				_, ok := <-snkCh
				if !ok {
					break
				}
			}
		}()

		testAfterCompletion := func() {
			close(snkCh)
		}

		pipeline.Start(ctx, testAfterCompletion)
	}

	cancel()
}
