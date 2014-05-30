package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"

	receiverRan = false
	receiver    = func(o Orbiter, ids []uint64) {
		receiverRan = true
		if orb, ok := o.(ReceiverOrbiter); ok {
			orb.SetReceiverIndex(ids[0] + 1)
		}
	}
	journalerRan = false
	journaler    = func(o Orbiter, ids []uint64) {
		journalerRan = true
		if orb, ok := o.(ReceiverOrbiter); ok {
			orb.SetJournalerIndex(ids[0])
		}
	}
	replicatorRan = false
	replicator    = func(o Orbiter, ids []uint64) {
		replicatorRan = true
		if orb, ok := o.(ReceiverOrbiter); ok {
			orb.SetReplicatorIndex(ids[0])
		}
	}
	unmarshallerRan = false
	unmarshaller    = func(o Orbiter, ids []uint64) {
		unmarshallerRan = true
		if orb, ok := o.(ReceiverOrbiter); ok {
			orb.SetUnmarshallerIndex(ids[0])
		}
	}
	executorRan = false
	executor    = func(o Orbiter, ids []uint64) {
		executorRan = true
		if orb, ok := o.(ReceiverOrbiter); ok {
			orb.SetExecutorIndex(ids[0])
		}
	}
)

func TestGetMessage(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(1)
	msg.marshalled = []byte(test + string(1))
	msg = orbiter.GetMessage(1 + buffer_size)
	assert.Equal(t, msg.marshalled, []byte(test+string(1)))
}

func TestGetBufferSize(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, buffer_size, orbiter.GetBufferSize())
}

func TestGetExecutorIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, uint64(0), orbiter.GetExecutorIndex())
}
