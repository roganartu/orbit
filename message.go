package orbit

import (
	"io"
	"sync/atomic"
)

// Message is the object that is stored in the Loop ring buffer.
// Each consumer must only have write access to a single field.
// This means the data will be duplicated, but it prevents unnecessary locking.
type Message struct {
	// ID of message. Monotonically increasing 64 bit unsigned int
	id uint64

	// Marshalled (raw) data as it was input into the loop
	marshalled *atomic.Value

	// Unmarshalled data.
	// Using an interface allows the package user to define what their
	// unmarshalled data stucture should look like.
	unmarshalled *atomic.Value

	// Where the result should be written to.
	// Can be any interface that has a Write([]byte) method available
	output io.Writer
}

func (m *Message) Init() {
	m.marshalled = &atomic.Value{}
	m.unmarshalled = &atomic.Value{}
}

func (m *Message) GetID() uint64 {
	return atomic.LoadUint64(&m.id)
}

func (m *Message) SetID(id uint64) {
	atomic.StoreUint64(&m.id, id)
}

func (m *Message) GetMarshalled() interface{} {
	return m.marshalled.Load()
}

func (m *Message) SetMarshalled(v interface{}) {
	m.marshalled.Store(v)
}

func (m *Message) GetUnmarshalled() interface{} {
	return m.unmarshalled.Load()
}

func (m *Message) SetUnmarshalled(obj interface{}) {
	m.unmarshalled.Store(obj)
}

func (m *Message) Write(b []byte) (int, error) {
	return m.output.Write(b)
}
