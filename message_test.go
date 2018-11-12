package orbit_test

import (
	"testing"

	"github.com/bmizerany/assert"

	target "github.com/roganartu/orbitus"
)

func TestGetMessage(t *testing.T) {
	loop := target.New(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := loop.GetMessage(1)
	msg.SetMarshalled([]byte(test + string(1)))
	msg = loop.GetMessage(1 + buffer_size)
	assert.Equal(t, msg.GetMarshalled(), []byte(test+string(1)))
}

func TestSetMessage(t *testing.T) {
	loop := target.New(buffer_size, nil, nil, nil, nil, nil)

	// Check out of bounds index wrapping
	msg := loop.GetMessage(1)
	msg.SetMarshalled([]byte(test))
	loop.SetMessage(1+buffer_size, msg)
	msg = loop.GetMessage(1)
	assert.Equal(t, msg.GetMarshalled(), []byte(test))
}

func TestMessageID(t *testing.T) {
	msg := target.Message{}
	msg.SetID(uint64(100))
	assert.Equal(t, msg.GetID(), uint64(100))
}

func TestMessageUnmarshalled(t *testing.T) {
	tmp := struct {
		Title string
	}{
		"testing",
	}
	msg := target.Message{}
	msg.Init()
	msg.SetUnmarshalled(tmp)
	assert.Equal(t, msg.GetUnmarshalled(), tmp)
}

func BenchmarkGetMessage(b *testing.B) {
	loop := target.New(buffer_size, nil, nil, nil, nil, nil)
	for i := 0; i < b.N; i++ {
		loop.GetMessage(uint64(i))
	}
}
