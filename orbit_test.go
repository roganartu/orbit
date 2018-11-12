package orbit_test

import (
	"sync"
	"testing"
	"time"

	"github.com/bmizerany/assert"
	"github.com/roganartu/orbitus/handlers"

	target "github.com/roganartu/orbitus"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"

	runCheckLock = &sync.Mutex{}

	// TODO pull these out into a mock subpackage
	receiverRan = false
	receiver    = func(p target.Processor, id uint64, val interface{}) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		receiverRan = true

		msg := p.GetMessage(id)
		msg.SetID(id)
		msg.SetMarshalled(val)
		p.SetMessage(id, msg)

		p.SetIndex(handlers.RECEIVER, id+1)
	}

	journalerRan = false
	journaler    = func(p target.Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		journalerRan = true
		handlers.Journaler(p, ids)
	}

	replicatorRan = false
	replicator    = func(p target.Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		replicatorRan = true
		handlers.Replicator(p, ids)
	}

	unmarshallerRan = false
	unmarshaller    = func(p target.Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		unmarshallerRan = true
		handlers.Unmarshaller(p, ids)
	}

	executorRan = false
	executor    = func(p target.Processor, ids []uint64) {
		runCheckLock.Lock()
		defer runCheckLock.Unlock()
		executorRan = true
		handlers.Executor(p, ids)
	}
)

func TestDefaultReceiver(t *testing.T) {
	loop := target.New(buffer_size, receiver, journaler, replicator, unmarshaller, executor)
	err := loop.Start()
	assert.Equal(t, nil, err)

	loop.Input <- []byte(test)
	time.Sleep(10 * time.Millisecond)

	loop.Stop()

	// Check out of bounds index wrapping
	msg := loop.GetMessage(4)
	assert.Equal(t, msg.GetMarshalled(), []byte(test))
}

func TestGetBufferSize(t *testing.T) {
	loop := target.New(buffer_size, nil, nil, nil, nil, nil)
	assert.Equal(t, buffer_size, loop.GetBufferSize())
}

func TestGetIndex(t *testing.T) {
	loop := target.New(buffer_size, nil, nil)
	assert.Equal(t, uint64(1), loop.GetIndex(0))
}

func TestNew(t *testing.T) {
	loop := target.New(buffer_size, nil, nil, nil, nil, nil)
	var i uint64 = 4

	// Ensure all the indexes are initialized to zero
	for j := 0; j < 5; j++ {
		assert.Equal(t, i-uint64(j), loop.GetIndex(j))
	}

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

	loop := target.New(buffer_size, receiver, journaler, replicator, unmarshaller, executor)
	err := loop.Start()
	assert.Equal(t, nil, err)

	// Manually add a new message to the receiver buffer to be processed
	// "{\"test\":\"This is a test message\"}" Base64 encoded
	loop.Input <- []byte("eyJ0ZXN0IjoiVGhpcyBpcyBhIHRlc3QgbWVzc2FnZSJ9")

	time.Sleep(10 * time.Millisecond)

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
	loop := target.New(buffer_size, receiver, journaler, replicator, unmarshaller, executor)
	var i uint64
	testvals := []uint64{1, buffer_size - 1, buffer_size, buffer_size + 1}

	for _, i = range testvals {
		err := loop.Reset(i)
		assert.Equal(t, nil, err)

		// Ensure all indexes have been set to the given value
		for j := 0; j < 5; j++ {
			assert.Equal(t, i-uint64(j), loop.GetIndex(j))
		}
	}

	// Ensure loop does not reset if running
	err := loop.Start()
	assert.Equal(t, nil, err)
	time.Sleep(1 * time.Millisecond)
	err = loop.Reset(buffer_size)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, "cannot reset a running Loop", err.Error())
}

func BenchmarkIntegrated(b *testing.B) {
	loop := target.New(buffer_size, receiver, journaler, replicator, unmarshaller, executor)
	receiverRan, journalerRan, replicatorRan, unmarshallerRan, executorRan = false, false, false, false, false
	err := loop.Start()
	if err != nil {
		b.Fatalf("Failed to start loop: %s", err)
	}
	defer loop.Stop()

	// Manually add a new message to the receiver buffer to be processed
	// "{\"test\":\"This is a test message\"}" Base64 encoded
	loop.Input <- []byte("eyJ0ZXN0IjoiVGhpcyBpcyBhIHRlc3QgbWVzc2FnZSJ9")

	time.Sleep(1 * time.Millisecond)

	for i := 0; i < b.N; i++ {
		loop.GetMessage(uint64(i))
	}
}
