package orbitus

import (
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"

	receiverRan = false
	receiver    = func(p Processor, id uint64, i interface{}) {
		receiverRan = true
		p.SetReceiverIndex(id + 1)
	}
	journalerRan = false
	journaler    = func(p Processor, ids []uint64) {
		journalerRan = true
		p.SetJournalerIndex(ids[0])
	}
	replicatorRan = false
	replicator    = func(p Processor, ids []uint64) {
		replicatorRan = true
		p.SetReplicatorIndex(ids[0])
	}
	unmarshallerRan = false
	unmarshaller    = func(p Processor, ids []uint64) {
		unmarshallerRan = true
		p.SetUnmarshallerIndex(ids[0])
	}
	executorRan = false
	executor    = func(p Processor, ids []uint64) {
		executorRan = true
		p.SetExecutorIndex(ids[0])
	}
)

func TestGetMessage(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(1)
	msg.marshalled = []byte(test + string(1))
	msg = orbiter.GetMessage(1 + buffer_size)
	assert.Equal(t, msg.marshalled, []byte(test+string(1)))
}

func TestDefaultReceiver(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)
	orbiter.Start()

	orbiter.Input <- []byte(test)
	time.Sleep(1 * time.Millisecond)

	orbiter.Stop()

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(4)
	assert.Equal(t, msg.GetMarshalled(), []byte(test))
}

func TestGetBufferSize(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, buffer_size, orbiter.GetBufferSize())
}

func TestGetExecutorIndex(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, uint64(0), orbiter.GetIndex(EXECUTOR))
}

func TestNewOrbiter(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var i uint64 = 4

	// Ensure all the indexes are initialized to zero
	assert.Equal(t, i, orbiter.GetIndex(RECEIVER))
	assert.Equal(t, i-1, orbiter.GetIndex(JOURNALER))
	assert.Equal(t, i-2, orbiter.GetIndex(REPLICATOR))
	assert.Equal(t, i-3, orbiter.GetIndex(UNMARSHALLER))
	assert.Equal(t, i-4, orbiter.GetIndex(EXECUTOR))

	// Ensure buffer has been fully allocated
	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		msg.SetMarshalled([]byte(test + string(i)))
	}

	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		assert.Equal(t, msg.GetMarshalled(), []byte(test+string(i)))
	}
}

func TestOrbiterStart(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, receiver, journaler, replicator,
		unmarshaller, executor)
	receiverRan, journalerRan, replicatorRan, unmarshallerRan, executorRan =
		false, false, false, false, false
	orbiter.Start()

	// Manually add a new message to the receiver buffer to be processed
	// "{\"test\":\"This is a test message\"}" Base64 encoded
	orbiter.Input <- []byte("eyJ0ZXN0IjoiVGhpcyBpcyBhIHRlc3QgbWVzc2FnZSJ9")

	time.Sleep(1 * time.Millisecond)

	orbiter.Stop()

	assert.Equal(t, true, receiverRan)
	assert.Equal(t, true, journalerRan)
	assert.Equal(t, true, replicatorRan)
	assert.Equal(t, true, unmarshallerRan)
	assert.Equal(t, true, executorRan)
}

func TestReceiverOrbiterReset(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var i uint64
	testvals := []uint64{1, buffer_size - 1, buffer_size, buffer_size + 1}

	for _, i = range testvals {
		err := orbiter.Reset(i)
		assert.Equal(t, nil, err)

		// Ensure all indexes have been set to the given value
		assert.Equal(t, i, orbiter.GetIndex(RECEIVER))
		assert.Equal(t, i-1, orbiter.GetIndex(JOURNALER))
		assert.Equal(t, i-2, orbiter.GetIndex(REPLICATOR))
		assert.Equal(t, i-3, orbiter.GetIndex(UNMARSHALLER))
		assert.Equal(t, i-4, orbiter.GetIndex(EXECUTOR))
	}

	// Ensure orbiter does not reset if running
	orbiter.running = true
	err := orbiter.Reset(buffer_size)
	assert.Equal(t, "Cannot reset a running Orbiter", err.Error())
}
