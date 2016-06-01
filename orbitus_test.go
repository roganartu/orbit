package orbitus

import (
	"testing"

	"github.com/bmizerany/assert"
)

var (
	buffer_size uint64 = 256 // 2^8
	test               = "Test string"
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
	assert.Equal(t, uint64(0), orbiter.GetIndex(EXECUTOR))
}
