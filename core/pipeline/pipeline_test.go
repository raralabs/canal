package pipeline

import (
	"context"
	content2 "github.com/raralabs/canal/core/message/content"
	"testing"
	"time"

	"github.com/raralabs/canal/core/message"
	"github.com/stretchr/testify/assert"
)

type numberGenerator struct {
	name   string
	curVal uint64
	maxVal uint64
}

func newNumberGenerator(maxVal uint64) Executor {
	return &numberGenerator{name: "Inline", curVal: 0, maxVal: maxVal}
}
func (s *numberGenerator) Execute(m message.Msg, proc IProcessorForExecutor) bool {
	if s.curVal >= s.maxVal {
		proc.Done()
		return true
	}

	s.curVal++
	content := content2.New()
	content = content.Add("value", content2.NewFieldValue(s.curVal, content2.INT))
	proc.Result(m, content, nil)
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
func (s *channelSink) Execute(m message.Msg, pr IProcessorForExecutor) bool {
	s.channel <- m
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
