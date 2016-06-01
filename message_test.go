package orbitus

import (
	"testing"

	"github.com/bmizerany/assert"
)

func TestGetMessage(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(1)
	msg.marshalled = []byte(test + string(1))
	msg = orbiter.GetMessage(1 + buffer_size)
	assert.Equal(t, msg.marshalled, []byte(test+string(1)))
}

func TestSetMessage(t *testing.T) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := orbiter.GetMessage(1)
	msg.SetMarshalled([]byte(test))
	orbiter.SetMessage(1+buffer_size, msg)
	msg = orbiter.GetMessage(1)
	assert.Equal(t, msg.GetMarshalled(), []byte(test))
}

func TestMessageID(t *testing.T) {
	msg := Message{}
	msg.SetID(uint64(100))
	assert.Equal(t, msg.GetID(), uint64(100))
}

func TestMessageMarshalled(t *testing.T) {
	msg := Message{}
	b := []byte{1, 2}
	msg.SetMarshalled(b)
	b[0] = 0
	assert.Equal(t, msg.GetMarshalled(), []byte{1, 2})
}

func TestMessageUnmarshalled(t *testing.T) {
	tmp := struct {
		Title string
	}{
		"testing",
	}
	msg := Message{}
	msg.SetUnmarshalled(tmp)
	assert.Equal(t, msg.GetUnmarshalled(), tmp)
}

func BenchmarkGetMessage(b *testing.B) {
	orbiter := NewOrbiter(buffer_size, nil, nil, nil, nil, nil)
	for i := 0; i < b.N; i++ {
		orbiter.GetMessage(uint64(i))
	}
}
