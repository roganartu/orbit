package orbitus

import (
	"errors"
)

// Consumer handler function.
// Defaults are provided, however it is expected most users will define their
// own functions. These functions will be called by the Orbiters upon launch
// and are expected to be long-running processes.
// They should remember the index of the last item they have processed. It is
// the function's responsibility to update the corresponding index via the
// appropriate Orbiter methods.
// It is assumed that all objects between the index stored in the Orbiter
// and the next consumer (backwards). Once this index is set the consumer behind
// will be allowed to process up to an including the new value.
// The second parameter is the index that the function should start processing
// from.
// If the consumers current index is equal to the index of the consumer ahead of
// it the consumer should do wait until this changes before processing any
// messages. This state should only really ever happen on startup, but it is
// safe practice for the system in any case.
type Handler func(Orbiter, uint64)

// The actual message that will be stored in the ring buffer.
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

// Notes about both the input and output orbiters below:
// The consumers are defined in the order they will be in the ring buffer,
// with the exception of the Business Logic Consumer which is last in the
// InputOrbiter and first in the OutputOrbiter.

// Base Orbiter
type Orbiter struct {
	// The actual buffer
	buffer []*Message

	// Size of the buffer
	// Must be a power of 2
	buffer_size uint64

	// Business Logic Consumer
	executorIndex   uint64
	executorHandler Handler
}

// Input buffer
type InputOrbiter struct {
	Orbiter

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

// Output buffer
type OutputOrbiter struct {
	Orbiter

	// Marshaller
	marshallerIndex   uint64
	marshallerHandler Handler

	// Publisher
	publisherIndex   uint64
	publisherHandler Handler
}

// Initializers

// Creates a new InputOrbiter
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
		Orbiter: Orbiter{
			buffer_size:     size,
			buffer:          make([]*Message, size),
			executorHandler: executor,
		},
	}

	// Create 'size' new Message objects and store them in the buffer
	var i uint64
	for i = 0; i < size; i++ {
		orbiter.buffer[i] = new(Message)
	}

	return orbiter
}

// Getters

// Get Message at given buffer address
func (o *Orbiter) getMessage(i uint64) (*Message, error) {
	// Bounds check
	if i >= o.buffer_size {
		err := errors.New("Message index out of range")
		return nil, err
	}

	return o.buffer[i], nil
}
