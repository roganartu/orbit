package orbit

import (
	"io"
)

// Message is the object that is stored in the Loop ring buffer.
// Each consumer must only have write access to a single field.
// This means the data will be duplicated, but it prevents unnecessary locking.
type Message struct {
	// ID of message. Monotonically increasing 64 bit unsigned int
	id uint64

	// Marshalled (raw) data.
	marshalled []byte

	// Unmarshalled data.
	// Using an interface allows the package user to define what their
	// unmarshalled data stucture should look like.
	unmarshalled interface{}

	// Where the result should be returned to.
	// Can be any interface that has a Write([]byte) method available
	output io.Writer
}

func (m *Message) GetID() uint64 {
	return m.id
}

func (m *Message) SetID(id uint64) {
	m.id = id
}

func (m *Message) GetMarshalled() []byte {
	return m.marshalled
}

func (m *Message) SetMarshalled(b []byte) {
	m.marshalled = make([]byte, len(b))
	copy(m.marshalled, b)
}

func (m *Message) GetUnmarshalled() interface{} {
	return m.unmarshalled
}

func (m *Message) SetUnmarshalled(obj interface{}) {
	m.unmarshalled = obj
}

func (m *Message) Write(b []byte) (int, error) {
	return m.output.Write(b)
}
