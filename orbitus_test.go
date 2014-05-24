package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"
)

// Initializers

func TestNewInputOrbiter(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Make sure buffer has been fully allocated
	var i uint64
	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		msg.marshalled = []byte(test + string(i))
	}

	for i = 0; i < buffer_size; i++ {
		msg := orbiter.GetMessage(i)
		assert.Equal(t, msg.marshalled, []byte(test+string(i)))
	}
}

// Getters

func TestGetMessage(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(1)
	msg.marshalled = []byte(test + string(1))
	msg = orbiter.GetMessage(1 + buffer_size)
	assert.Equal(t, msg.marshalled, []byte(test+string(1)))
}
