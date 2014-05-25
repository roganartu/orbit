package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

var (
	buffer_size  uint64 = 256 // 2^8
	test                = "Test string"
	journalerRan        = false
	journaler           = func(o Orbiter, ids []uint64) {
		journalerRan = true
		if orb, ok := o.(InputOrbiter); ok {
			orb.SetJournalerIndex(ids[0])
		}
	}
	replicatorRan = false
	replicator    = func(o Orbiter, ids []uint64) {
		replicatorRan = true
		if orb, ok := o.(InputOrbiter); ok {
			orb.SetReplicatorIndex(ids[0])
		}
	}
	unmarshallerRan = false
	unmarshaller    = func(o Orbiter, ids []uint64) {
		unmarshallerRan = true
		if orb, ok := o.(InputOrbiter); ok {
			orb.SetUnmarshallerIndex(ids[0])
		}
	}
	executorRan = false
	executor    = func(o Orbiter, ids []uint64) {
		executorRan = true
		if orb, ok := o.(InputOrbiter); ok {
			orb.SetExecutorIndex(ids[0])
		}
	}
)

func TestNewInputOrbiter(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var i uint64 = 4

	// Ensure all the indexes are initialized to zero
	assert.Equal(t, i, orbiter.GetReceiverIndex())
	assert.Equal(t, i-1, orbiter.GetJournalerIndex())
	assert.Equal(t, i-2, orbiter.GetReplicatorIndex())
	assert.Equal(t, i-3, orbiter.GetUnmarshallerIndex())
	assert.Equal(t, i-4, orbiter.GetExecutorIndex())

	// Ensure buffer has been fully allocated
	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		msg.marshalled = []byte(test + string(i))
	}

	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		assert.Equal(t, msg.marshalled, []byte(test+string(i)))
	}
}

func TestGetMessage(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(1)
	msg.marshalled = []byte(test + string(1))
	msg = orbiter.GetMessage(1 + buffer_size)
	assert.Equal(t, msg.marshalled, []byte(test+string(1)))
}

func TestInputOrbiterReset(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var i uint64
	testvals := []uint64{1, buffer_size - 1, buffer_size, buffer_size + 1}

	for _, i = range testvals {
		err := orbiter.Reset(i)
		assert.Equal(t, nil, err)

		// Ensure all indexes have been set to the given value
		assert.Equal(t, i, orbiter.GetReceiverIndex())
		assert.Equal(t, i-1, orbiter.GetJournalerIndex())
		assert.Equal(t, i-2, orbiter.GetReplicatorIndex())
		assert.Equal(t, i-3, orbiter.GetUnmarshallerIndex())
		assert.Equal(t, i-4, orbiter.GetExecutorIndex())
	}

	// Ensure orbiter does not reset if running
	orbiter.running = true
	err := orbiter.Reset(buffer_size)
	assert.Equal(t, "Cannot reset a running Orbiter", err.Error())
}

func TestInputOrbiterStart(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, journaler, replicator,
		unmarshaller, executor)
	journalerRan, replicatorRan, unmarshallerRan, executorRan =
		false, false, false, false
	orbiter.Start()

	// Manually insert a new Message
	orbiter.buffer[4] = &Message{
		id:         4,
		marshalled: []byte("This is a test message"),
	}
	orbiter.receiverIndex += 1

	time.Sleep(1 * time.Millisecond)

	assert.Equal(t, true, journalerRan)
	assert.Equal(t, true, replicatorRan)
	assert.Equal(t, true, unmarshallerRan)
	assert.Equal(t, true, executorRan)
}
