package functions

import (
	"fmt"
	"log"

	"github.com/cespare/xxhash"
	"github.com/clarkduvall/hyperloglog"
	"github.com/raralabs/canal/core/message"
	"github.com/raralabs/canal/core/transforms/agg"
)

type xxHash struct {
	hash uint64
}

func newxxHash(v interface{}) *xxHash {
	return &xxHash{
		hash: xxhash.Sum64String(fmt.Sprintf("%v", v)),
	}
}

func (xxh *xxHash) Sum64() uint64 {
	return xxh.hash
}

type HLLpp struct {
	tmpl  agg.IAggFuncTemplate
	hllpp *hyperloglog.HyperLogLogPlus
}

func NewHLLpp(tmpl agg.IAggFuncTemplate) *HLLpp {
	hpp, err := hyperloglog.NewPlus(8)
	if err != nil {
		log.Panic(err)
	}

	return &HLLpp{
		tmpl:  tmpl,
		hllpp: hpp,
	}
}

func (H *HLLpp) Add(content, prevContent *message.OrderedContent) {
	if H.tmpl.Filter(content.Values()) {
		val, ok := content.Get(H.tmpl.Field())
		if !ok {
			return
		}

		H.hllpp.Add(newxxHash(val.Value()))
	}
}

func (H *HLLpp) Result() *message.MsgFieldValue {
	return message.NewFieldValue(H.hllpp.Count(), message.INT)
}

func (H *HLLpp) Name() string {
	return H.tmpl.Name()
}

func (H *HLLpp) Reset() {
	H.hllpp.Clear()
}
