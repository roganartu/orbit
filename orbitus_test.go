package orbitus

import (
	"github.com/bmizerany/assert"
	"testing"
)

var (
	buffer_size uint64 = 256 // 2^8
)

// Initializers

func TestNewInputOrbiter(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)
	test := "Test string"

	// Make sure buffer has been fully allocated
	var i uint64
	for i = 0; i < buffer_size; i++ {
		msg, err := orbiter.getMessage(i)
		assert.Equal(t, nil, err)
		msg.marshalled = []byte(test + string(i))
	}

	for i = 0; i < buffer_size; i++ {
		msg, err := orbiter.getMessage(i)
		assert.Equal(t, nil, err)
		assert.Equal(t, msg.marshalled, []byte(test+string(i)))
	}
}

// Getters

func TestGetMessage(t *testing.T) {
	orbiter := NewInputOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds error
	msg, err := orbiter.getMessage(buffer_size)
	assert.Equal(t, (*Message)(nil), msg)
	assert.Equal(t, "Message index out of range", err.Error())
}
