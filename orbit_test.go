package orbit

import (
	"sync"
	"testing"
	"time"

	"github.com/bmizerany/assert"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"

	runCheckLock = &sync.Mutex{}

	receiverRan = false
	receiver    = func(p Processor, id uint64, i interface{}) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		receiverRan = true
		p.SetReceiverIndex(id + 1)
	}
	journalerRan = false
	journaler    = func(p Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		journalerRan = true
		p.SetJournalerIndex(ids[0])
	}
	replicatorRan = false
	replicator    = func(p Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		replicatorRan = true
		p.SetReplicatorIndex(ids[0])
	}
	unmarshallerRan = false
	unmarshaller    = func(p Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		unmarshallerRan = true
		p.SetUnmarshallerIndex(ids[0])
	}
	executorRan = false
	executor    = func(p Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		executorRan = true
		p.SetExecutorIndex(ids[0])
	}
)

func TestDefaultReceiver(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	loop.Start()

	loop.Input <- []byte(test)
	time.Sleep(1 * time.Millisecond)

	loop.Stop()

	// Check out of bounds index wrapping
	msg := loop.GetMessage(4)
	assert.Equal(t, msg.GetMarshalled(), []byte(test))
}

func TestGetBufferSize(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, buffer_size, loop.GetBufferSize())
}

func TestGetExecutorIndex(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, uint64(0), loop.GetIndex(EXECUTOR))
}

func TestNew(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	var i uint64 = 4

	// Ensure all the indexes are initialized to zero
	assert.Equal(t, i, loop.GetIndex(RECEIVER))
	assert.Equal(t, i-1, loop.GetIndex(JOURNALER))
	assert.Equal(t, i-2, loop.GetIndex(REPLICATOR))
	assert.Equal(t, i-3, loop.GetIndex(UNMARSHALLER))
	assert.Equal(t, i-4, loop.GetIndex(EXECUTOR))

	// Ensure buffer has been fully allocated
	for i = 0; i < buffer_size; i++ {
		msg := loop.GetMessage(i)
		msg.SetMarshalled([]byte(test + string(i)))
	}

	for i = 0; i < buffer_size; i++ {
		msg := loop.GetMessage(i)
		assert.Equal(t, msg.GetMarshalled(), []byte(test+string(i)))
	}
}

func TestLoopStart(t *testing.T) {
	runCheckLock.Lock()
	receiverRan, journalerRan, replicatorRan, unmarshallerRan, executorRan =
		false, false, false, false, false
	runCheckLock.Unlock()

	loop := New(buffer_size, receiver, journaler, replicator,
		unmarshaller, executor)
	loop.Start()

	// Manually add a new message to the receiver buffer to be processed
	// "{\"test\":\"This is a test message\"}" Base64 encoded
	loop.Input <- []byte("eyJ0ZXN0IjoiVGhpcyBpcyBhIHRlc3QgbWVzc2FnZSJ9")

	time.Sleep(1 * time.Millisecond)

	loop.Stop()

	runCheckLock.Lock()
	defer runCheckLock.Unlock()
	assert.Equal(t, true, receiverRan)
	assert.Equal(t, true, journalerRan)
	assert.Equal(t, true, replicatorRan)
	assert.Equal(t, true, unmarshallerRan)
	assert.Equal(t, true, executorRan)
}

func TestReceiverLoopReset(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	var i uint64
	testvals := []uint64{1, buffer_size - 1, buffer_size, buffer_size + 1}

	for _, i = range testvals {
		err := loop.Reset(i)
		assert.Equal(t, nil, err)

		// Ensure all indexes have been set to the given value
		assert.Equal(t, i, loop.GetIndex(RECEIVER))
		assert.Equal(t, i-1, loop.GetIndex(JOURNALER))
		assert.Equal(t, i-2, loop.GetIndex(REPLICATOR))
		assert.Equal(t, i-3, loop.GetIndex(UNMARSHALLER))
		assert.Equal(t, i-4, loop.GetIndex(EXECUTOR))
	}

	// Ensure loop does not reset if running
	loop.running = true
	err := loop.Reset(buffer_size)
	assert.Equal(t, "Cannot reset a running Loop", err.Error())
}

func BenchmarkIntegrated(b *testing.B) {
	loop := New(buffer_size, receiver, journaler, replicator,
		unmarshaller, executor)
	receiverRan, journalerRan, replicatorRan, unmarshallerRan, executorRan =
		false, false, false, false, false
	loop.Start()
	defer loop.Stop()

	// Manually add a new message to the receiver buffer to be processed
	// "{\"test\":\"This is a test message\"}" Base64 encoded
	loop.Input <- []byte("eyJ0ZXN0IjoiVGhpcyBpcyBhIHRlc3QgbWVzc2FnZSJ9")

	time.Sleep(1 * time.Millisecond)

	for i := 0; i < b.N; i++ {
		loop.GetMessage(uint64(i))
	}
}
