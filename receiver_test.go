package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
	"time"
)

func TestNewReceiverOrbiter(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
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

func TestReceiverOrbiterReset(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
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

func TestReceiverOrbiterStart(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, journaler, replicator,
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

func TestReceiverOrbiterSetReceiverIndex(t *testing.T) {
	orbiter := NewReceiverOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = orbiter.GetReceiverIndex()
	err = orbiter.SetReceiverIndex(orbiter.GetReceiverIndex() - 1)
	assert.Equal(t, "New receiver index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, orbiter.GetReceiverIndex())

	// Setting to same value as current should work (although do nothing really)
	old = orbiter.GetReceiverIndex()
	err = orbiter.SetReceiverIndex(orbiter.GetReceiverIndex())
	assert.Equal(t, nil, err)
	assert.Equal(t, old, orbiter.GetReceiverIndex())

	// Basic incrementing by one (where there is room in front) should work.
	old = orbiter.GetReceiverIndex()
	err = orbiter.SetReceiverIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, orbiter.GetReceiverIndex())

	// Wrapping around should work, even though new modulus of index is lower
	// than the index of the consumer in front of it.
	orbiter.Reset(5)
	assert.Equal(t, uint64(1), orbiter.GetExecutorIndex())
	err = orbiter.SetReceiverIndex(buffer_size)
	assert.Equal(t, nil, err)
	assert.Equal(t, buffer_size, orbiter.GetReceiverIndex())

	// Wrapping around and setting index to the same location as the Business
	// Logic Consumer should not work.
	orbiter.Reset(5)
	assert.Equal(t, uint64(1), orbiter.GetExecutorIndex())
	old = orbiter.GetReceiverIndex()
	err = orbiter.SetReceiverIndex(buffer_size + 1)
	assert.Equal(t, "The Receiver Consumer cannot pass the Business Logic "+
		"Consumer", err.Error())
	assert.Equal(t, old, orbiter.GetReceiverIndex())

	// Wrapping around and passing the Business Logic Consumer should not work.
	orbiter.Reset(5)
	assert.Equal(t, uint64(1), orbiter.GetExecutorIndex())
	old = orbiter.GetReceiverIndex()
	err = orbiter.SetReceiverIndex(buffer_size + (buffer_size / 2))
	assert.Equal(t, "The Receiver Consumer cannot pass the Business Logic "+
		"Consumer", err.Error())
	assert.Equal(t, old, orbiter.GetReceiverIndex())
}
