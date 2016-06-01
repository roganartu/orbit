// Package orbitus provides an implementation of the LMAX Disruptor.
//
// For more info on LMAX see Martin Fowler's blog post: http://martinfowler.com/articles/lmax.html
//
// Alternatively read the paper: http://disruptor.googlecode.com/files/Disruptor-1.0.pdf
package orbitus

import (
	"io"
)

// Handler is the Consumer handler function type.
// Defaults are provided, however it is expected most users will define their
// own functions. It will be called by the Orbiters when new messages are
// available for processing. It is not expected to be a long-running process.
//
// It should remember the index of the last item it has processed. It is the
// Handler's responsibility to update the corresponding index via the
// appropriate Orbiter methods once processing of a Message is complete.
//
// It is assumed that all objects between the index stored in the Orbiter
// and the next consumer (backwards). Once this index is set the consumer behind
// will be allowed to process up to an including the new value.
// The second parameter is an array of indexes for the handler to process.
//
// If the consumers current index is equal to the index of the consumer ahead of
// it the consumer should wait until this changes before processing any
// messages. This state should only really ever happen on startup or reset, but
// it is safe practice for the system in any case.
type Handler func(Orbiter, []uint64)

// Message is the object that is stored in the Orbiter ring buffer.
// Each consumer must only have write access to a single field.
// This means the data will be duplicated, but it prevents unnecessary locking.
type Message struct {
	// ID of message. Monotonically increasing 64 bit unsigned int
	id uint64

	// Marshalled data.
	// Basically just a JSON string as a byte slice
	marshalled []byte

	// Unmarshalled data.
	// Using an interface allows the package user to define what their
	// unmarshalled data stucture should look like.
	unmarshalled interface{}

	// Where the result should be returned to.
	// Can be any interface that has a Write([]byte) method available
	output io.Writer
}

type Orbiter interface {
	// Start the Orbiter.
	// Launches goroutines and opens communication channel to start
	// receiving messages on.
	Start()

	// Reset the Orbiter to a given index.
	// Cannot be called while Orbiter is running. Stop with
	// Orbiter.Stop() before resetting.
	Reset(i uint64) error

	// Gets a pointer to the message at a given index.
	// Wraps around the ring buffer if necessary.
	GetMessage(i uint64) *Message

	// Get size of buffer
	GetBufferSize() uint64

	// Get current index of the given Consumer
	GetIndex(int) uint64
}

type ReceiverOrbiter interface {
	// Set the receiver index to the given value
	SetReceiverIndex(uint64) error

	// Set the journaler index to the given value
	SetJournalerIndex(uint64) error

	// Set the replicator index to the given value
	SetReplicatorIndex(uint64) error

	// Set the unmarshaller index to the given value
	SetUnmarshallerIndex(uint64) error

	// Set index of Business Logic Consumer to the given value
	SetExecutorIndex(uint64) error
}

// Orbiter maintains a buffer and Business Logic Consumer handler.
//
// It is included in both inputOrbiter and outputOrbiter.
//
// In both the inputOrbiter and outputOrbiter objects the consumers are
// defined in the order they will be in the ring buffer, with the exception
// of the Business Logic Consumer which is last in the inputOrbiter and first
// in the outputOrbiter.
type orbiter struct {
	// The actual buffer
	buffer []*Message

	// Size of the buffer
	// Must be a power of 2
	buffer_size uint64

	// Indexes, handlers and channels for all consumers
	index   []uint64
	handler []Handler
	channel []chan int

	// Flag to indicate whether Orbiter is running
	running bool
}

// outputOrbiter marshals Business Logic Consumer output and sends it to clients.
type outputOrbiter struct {
	orbiter

	// Marshaller
	marshallerIndex   uint64
	marshallerHandler Handler

	// Publisher
	publisherIndex   uint64
	publisherHandler Handler
}

// GetMessage returns the message at the given address in the buffer.
//
// If the provided index is larger than the buffer size then the modulus is
// used to generate an index that is in range.
func (o *orbiter) GetMessage(i uint64) *Message {
	// Bounds check
	if i >= o.buffer_size {
		i = i % o.buffer_size
	}

	return o.buffer[i]
}

// GetBufferSize returns the size of the Orbiter's Message buffer array.
func (o *orbiter) GetBufferSize() uint64 {
	return o.buffer_size
}
