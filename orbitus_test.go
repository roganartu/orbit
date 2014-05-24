package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"
)

func TestNewInputOrbiter(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)
	var i uint64 = 0

	// Ensure all the indexes are initialized to zero
	assert.Equal(t, i, orbiter.GetReceiverIndex())
	assert.Equal(t, i, orbiter.GetJournalerIndex())
	assert.Equal(t, i, orbiter.GetReplicatorIndex())
	assert.Equal(t, i, orbiter.GetUnmarshallerIndex())
	assert.Equal(t, i, orbiter.GetExecutorIndex())

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
		assert.Equal(t, i, orbiter.GetJournalerIndex())
		assert.Equal(t, i, orbiter.GetReplicatorIndex())
		assert.Equal(t, i, orbiter.GetUnmarshallerIndex())
		assert.Equal(t, i, orbiter.GetExecutorIndex())
	}

	// Ensure orbiter does not reset if running
	orbiter.running = true
	err := orbiter.Reset(buffer_size)
	assert.Equal(t, "Cannot reset a running Orbiter", err.Error())
}
