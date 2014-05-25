// Package orbitus provides an implementation of the LMAX Disruptor.
//
// For more info on LMAX see Martin Fowler's blog post: http://martinfowler.com/articles/lmax.html
//
// Alternatively read the paper: http://disruptor.googlecode.com/files/Disruptor-1.0.pdf
package orbitus

import (
	"errors"
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
type Handler func(*Orbiter, []uint64)

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
	output interface{}
}

type Orbiter interface {
	// Start the Orbiter.
	// Launches goroutines and opens communication channel to start
	// receiving messages on.
	Start()

	// Reset the Orbiter to a given index.
	// Cannot be called while Orbiter is running. Stop with
	// Orbiter.Stop() before resetting.
	Reset(i uint64)

	// Gets a pointer to the message at a given index.
	// Wraps around the ring buffer if necessary.
	GetMessage(i uint64) *Message

	// Get size of buffer
	GetBufferSize() uint64

	// Get current index of Business Logic Consumer
	GetExecutorIndex() uint64
}

// Orbiter maintains a buffer and Business Logic Consumer handler.
//
// It is included in both InputOrbiter and OutputOrbiter.
//
// In both the InputOrbiter and OutputOrbiter objects the consumers are
// defined in the order they will be in the ring buffer, with the exception
// of the Business Logic Consumer which is last in the InputOrbiter and first
// in the OutputOrbiter.
type orbiter struct {
	// The actual buffer
	buffer []*Message

	// Size of the buffer
	// Must be a power of 2
	buffer_size uint64

	// Business Logic Consumer
	executorIndex   uint64
	executorHandler Handler

	// Flag to indicate whether Orbiter is running
	running bool
}

// InputOrbiter unmarshals messages and coordinates journalling and replication.
type InputOrbiter struct {
	orbiter

	// Receiver
	receiverIndex   uint64
	receiverHandler Handler

	// Journaler
	journalerIndex   uint64
	journalerHandler Handler

	// Replicator
	replicatorIndex   uint64
	replicatorHandler Handler

	// Unmarshaller
	unmarshallerIndex   uint64
	unmarshallerHandler Handler
}

// OutputOrbiter marshals Business Logic Consumer output and sends it to clients.
type OutputOrbiter struct {
	orbiter

	// Marshaller
	marshallerIndex   uint64
	marshallerHandler Handler

	// Publisher
	publisherIndex   uint64
	publisherHandler Handler
}

// NewInputOrbiter initializes a new InputOrbiter.
//
// All indexes are set to 0 and Handlers are assigned.
//
// Space for the buffer is allocated and is filled with empty Message objects.
//
// It returns a pointer to the initialized InputOrbiter.
func NewInputOrbiter(
	size uint64,
	receiver Handler,
	journaler Handler,
	replicator Handler,
	unmarshaller Handler,
	executor Handler,
) *InputOrbiter {
	orbiter := &InputOrbiter{
		// All the indexes start at zero. The consumers should do nothing in the
		// case that their index is the same as the consumer ahead of them.
		receiverIndex:     0,
		journalerIndex:    0,
		replicatorIndex:   0,
		unmarshallerIndex: 0,

		// Handlers
		receiverHandler:     receiver,
		journalerHandler:    journaler,
		replicatorHandler:   replicator,
		unmarshallerHandler: unmarshaller,

		// Allocate the buffer
		orbiter: orbiter{
			buffer_size:     size,
			buffer:          make([]*Message, size),
			executorHandler: executor,

			// Start in stopped state
			running: false,
		},
	}

	// Create 'size' new Message objects and store them in the buffer
	var i uint64
	for i = 0; i < size; i++ {
		orbiter.buffer[i] = new(Message)
	}

	return orbiter
}

// Reset sets all indexes to a given value.
// This is useful for rebuilding Orbiter state from an input file
// (eg: journaled output) instead of manually looping through the buffer until
// the desired index is reached.
//
// Returns an error if called while Orbiter is running. Stop Orbiter with
// Orbiter.Stop() before resetting.
func (o *InputOrbiter) Reset(i uint64) error {
	if o.running {
		return errors.New("Cannot reset a running Orbiter")
	}

	// Bypass the setters otherwise their sanity checks will error
	o.receiverIndex = i
	o.journalerIndex = i
	o.replicatorIndex = i
	o.unmarshallerIndex = i
	o.executorIndex = i

	return nil
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

// GetExecutorIndex returns the Orbiter's current executorIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *orbiter) GetExecutorIndex() uint64 {
	return o.executorIndex
}

// GetReceiverIndex returns the InputOrbiter's current receiverIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *InputOrbiter) GetReceiverIndex() uint64 {
	return o.receiverIndex
}

// GetJournalerIndex returns the InputOrbiter's current journalerIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *InputOrbiter) GetJournalerIndex() uint64 {
	return o.journalerIndex
}

// GetUnmarshallerIndex returns the InputOrbiter's current unmarshallerIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *InputOrbiter) GetUnmarshallerIndex() uint64 {
	return o.unmarshallerIndex
}

// GetReplicatorIndex returns the InputOrbiter's current replicatorIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *InputOrbiter) GetReplicatorIndex() uint64 {
	return o.replicatorIndex
}
