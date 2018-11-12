package handlers

import (
	"fmt"

	orbit "github.com/roganartu/orbitus"
)

const (
	RECEIVER = iota
	JOURNALER
	REPLICATOR
	UNMARSHALLER
	EXECUTOR
)

func Receiver(p orbit.Processor, id uint64, obj interface{}) {
	// Store message and current index
	elem := p.GetMessage(id)
	elem.SetID(id)

	if b, ok := obj.([]byte); ok {
		elem.SetMarshalled(b)
	} else {
		panic(fmt.Sprintf("received non-byte-slice object: %+v", obj))
	}

	p.SetMessage(id, elem)
	p.SetIndex(RECEIVER, id+1)
}

func Journaler(p orbit.Processor, ids []uint64) {
	p.SetIndex(JOURNALER, ids[len(ids)-1])
}

func Replicator(p orbit.Processor, ids []uint64) {
	p.SetIndex(REPLICATOR, ids[len(ids)-1])
}

func Unmarshaller(p orbit.Processor, ids []uint64) {
	p.SetIndex(UNMARSHALLER, ids[len(ids)-1])
}

func Executor(p orbit.Processor, ids []uint64) {
	p.SetIndex(EXECUTOR, ids[len(ids)-1])
}
