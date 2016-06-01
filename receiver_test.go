package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

var (
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

func TestNewReceiverOrbiter(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
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
		msg.marshalled = []byte(test + string(i))
	}

	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		assert.Equal(t, msg.marshalled, []byte(test+string(i)))
	}
}

func TestReceiverOrbiterReset(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
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

func TestReceiverOrbiterStart(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, receiver, journaler, replicator,
		unmarshaller, executor)
	receiverRan, journalerRan, replicatorRan, unmarshallerRan, executorRan =
		false, false, false, false, false
	orbiter.Start()

	// Manually add a new message to the receiver buffer to be processed
	// "{\"test\":\"This is a test message\"}" Base64 encoded
	orbiter.receiverBuffer <- []byte("eyJ0ZXN0IjoiVGhpcyBpcyBhIHRlc3QgbWVzc2FnZSJ9")

	time.Sleep(1 * time.Millisecond)

	orbiter.Stop()

	assert.Equal(t, true, receiverRan)
	assert.Equal(t, true, journalerRan)
	assert.Equal(t, true, replicatorRan)
	assert.Equal(t, true, unmarshallerRan)
	assert.Equal(t, true, executorRan)
}

func TestReceiverOrbiterSetReceiverIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = orbiter.GetIndex(RECEIVER)
	err = orbiter.SetReceiverIndex(orbiter.GetIndex(RECEIVER) - 1)
	assert.Equal(t, "New receiver index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, orbiter.GetIndex(RECEIVER))

	// Setting to same value as current should work (although do nothing really)
	old = orbiter.GetIndex(RECEIVER)
	err = orbiter.SetReceiverIndex(orbiter.GetIndex(RECEIVER))
	assert.Equal(t, nil, err)
	assert.Equal(t, old, orbiter.GetIndex(RECEIVER))

	// Basic incrementing by one (where there is room in front) should work.
	old = orbiter.GetIndex(RECEIVER)
	err = orbiter.SetReceiverIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, orbiter.GetIndex(RECEIVER))

	// Wrapping around should work, even though new modulus of index is lower
	// than the index of the consumer in front of it.
	orbiter.Reset(5)
	assert.Equal(t, uint64(1), orbiter.GetIndex(EXECUTOR))
	err = orbiter.SetReceiverIndex(buffer_size)
	assert.Equal(t, nil, err)
	assert.Equal(t, buffer_size, orbiter.GetIndex(RECEIVER))

	// Wrapping around and setting index to the same location as the Business
	// Logic Consumer should not work.
	orbiter.Reset(5)
	assert.Equal(t, uint64(1), orbiter.GetIndex(EXECUTOR))
	old = orbiter.GetIndex(RECEIVER)
	err = orbiter.SetReceiverIndex(buffer_size + 1)
	assert.Equal(t, "The Receiver Consumer cannot pass the Business Logic "+
		"Consumer", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(RECEIVER))

	// Wrapping around and passing the Business Logic Consumer should not work.
	orbiter.Reset(5)
	assert.Equal(t, uint64(1), orbiter.GetIndex(EXECUTOR))
	old = orbiter.GetIndex(RECEIVER)
	err = orbiter.SetReceiverIndex(buffer_size + (buffer_size / 2))
	assert.Equal(t, "The Receiver Consumer cannot pass the Business Logic "+
		"Consumer", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(RECEIVER))
}

func TestReceiverOrbiterSetJournalerIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = orbiter.GetIndex(JOURNALER)
	err = orbiter.SetJournalerIndex(orbiter.GetIndex(JOURNALER) - 1)
	assert.Equal(t, "New journaler index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, orbiter.GetIndex(JOURNALER))

	// Setting to same value as current should work (although do nothing really)
	old = orbiter.GetIndex(JOURNALER)
	err = orbiter.SetJournalerIndex(orbiter.GetIndex(JOURNALER))
	assert.Equal(t, nil, err)
	assert.Equal(t, old, orbiter.GetIndex(JOURNALER))

	// Basic incrementing by one (where there is room in front) should work.
	orbiter.SetReceiverIndex(orbiter.GetIndex(RECEIVER) + 1)
	old = orbiter.GetIndex(JOURNALER)
	err = orbiter.SetJournalerIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, orbiter.GetIndex(JOURNALER))

	// Setting index to the same as the Receiver Consumer should not work.
	old = orbiter.GetIndex(JOURNALER)
	err = orbiter.SetJournalerIndex(orbiter.GetIndex(RECEIVER))
	assert.Equal(t, "New journaler index cannot be greater than the current "+
		"receiver index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(JOURNALER))

	// Passing the Receiver Consumer should not work
	old = orbiter.GetIndex(JOURNALER)
	err = orbiter.SetJournalerIndex(orbiter.GetIndex(RECEIVER) +
		orbiter.GetBufferSize() - 1)
	assert.Equal(t, "New journaler index cannot be greater than the current "+
		"receiver index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(JOURNALER))
}

func TestReceiverOrbiterSetReplicatorIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = orbiter.GetIndex(REPLICATOR)
	err = orbiter.SetReplicatorIndex(old - 1)
	assert.Equal(t, "New replicator index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, orbiter.GetIndex(REPLICATOR))

	// Setting to same value as current should work (although do nothing really)
	old = orbiter.GetIndex(REPLICATOR)
	err = orbiter.SetReplicatorIndex(old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, orbiter.GetIndex(REPLICATOR))

	// Basic incrementing by one (where there is room in front) should work.
	orbiter.SetReceiverIndex(orbiter.GetIndex(RECEIVER) + 1)
	orbiter.SetJournalerIndex(orbiter.GetIndex(JOURNALER) + 1)
	old = orbiter.GetIndex(REPLICATOR)
	err = orbiter.SetReplicatorIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, orbiter.GetIndex(REPLICATOR))

	// Setting index to the same as the Journaler Consumer should not work.
	old = orbiter.GetIndex(REPLICATOR)
	err = orbiter.SetReplicatorIndex(orbiter.GetIndex(JOURNALER))
	assert.Equal(t, "New replicator index cannot be greater than the current "+
		"journaler index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(REPLICATOR))

	// Passing the Journaler Consumer should not work
	old = orbiter.GetIndex(REPLICATOR)
	err = orbiter.SetReplicatorIndex(orbiter.GetIndex(JOURNALER) +
		orbiter.GetBufferSize() - 1)
	assert.Equal(t, "New replicator index cannot be greater than the current "+
		"journaler index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(REPLICATOR))
}

func TestReceiverOrbiterSetUnmarshallerIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = orbiter.GetIndex(UNMARSHALLER)
	err = orbiter.SetUnmarshallerIndex(old - 1)
	assert.Equal(t, "New unmarshaller index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, orbiter.GetIndex(UNMARSHALLER))

	// Setting to same value as current should work (although do nothing really)
	old = orbiter.GetIndex(UNMARSHALLER)
	err = orbiter.SetUnmarshallerIndex(old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, orbiter.GetIndex(UNMARSHALLER))

	// Basic incrementing by one (where there is room in front) should work.
	orbiter.SetReceiverIndex(orbiter.GetIndex(RECEIVER) + 1)
	orbiter.SetJournalerIndex(orbiter.GetIndex(JOURNALER) + 1)
	orbiter.SetReplicatorIndex(orbiter.GetIndex(REPLICATOR) + 1)
	old = orbiter.GetIndex(UNMARSHALLER)
	err = orbiter.SetUnmarshallerIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, orbiter.GetIndex(UNMARSHALLER))

	// Setting index to the same as the Replicator Consumer should not work.
	old = orbiter.GetIndex(UNMARSHALLER)
	err = orbiter.SetUnmarshallerIndex(orbiter.GetIndex(REPLICATOR))
	assert.Equal(t, "New unmarshaller index cannot be greater than the current "+
		"replicator index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(UNMARSHALLER))

	// Passing the Replicator Consumer should not work
	old = orbiter.GetIndex(UNMARSHALLER)
	err = orbiter.SetUnmarshallerIndex(orbiter.GetIndex(REPLICATOR) +
		orbiter.GetBufferSize() - 1)
	assert.Equal(t, "New unmarshaller index cannot be greater than the current "+
		"replicator index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(UNMARSHALLER))
}

func TestReceiverOrbiterSetExecutorIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	orbiter.Reset(5)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = orbiter.GetIndex(EXECUTOR)
	err = orbiter.SetExecutorIndex(old - 1)
	assert.Equal(t, "New executor index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, orbiter.GetIndex(EXECUTOR))

	// Setting to same value as current should work (although do nothing really)
	old = orbiter.GetIndex(EXECUTOR)
	err = orbiter.SetExecutorIndex(old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, orbiter.GetIndex(EXECUTOR))

	// Basic incrementing by one (where there is room in front) should work.
	orbiter.SetReceiverIndex(orbiter.GetIndex(RECEIVER) + 1)
	orbiter.SetJournalerIndex(orbiter.GetIndex(JOURNALER) + 1)
	orbiter.SetReplicatorIndex(orbiter.GetIndex(REPLICATOR) + 1)
	orbiter.SetUnmarshallerIndex(orbiter.GetIndex(UNMARSHALLER) + 1)
	old = orbiter.GetIndex(EXECUTOR)
	err = orbiter.SetExecutorIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, orbiter.GetIndex(EXECUTOR))

	// Setting index to the same as the Unmarshaller Consumer should not work.
	old = orbiter.GetIndex(EXECUTOR)
	err = orbiter.SetExecutorIndex(orbiter.GetIndex(UNMARSHALLER))
	assert.Equal(t, "New executor index cannot be greater than the current "+
		"unmarshaller index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(EXECUTOR))

	// Passing the Unmarshaller Consumer should not work
	old = orbiter.GetIndex(EXECUTOR)
	err = orbiter.SetExecutorIndex(orbiter.GetIndex(UNMARSHALLER) +
		orbiter.GetBufferSize() - 1)
	assert.Equal(t, "New executor index cannot be greater than the current "+
		"unmarshaller index", err.Error())
	assert.Equal(t, old, orbiter.GetIndex(EXECUTOR))
}
