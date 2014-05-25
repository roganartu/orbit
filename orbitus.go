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
	Reset(i uint64) error

	// Gets a pointer to the message at a given index.
	// Wraps around the ring buffer if necessary.
	GetMessage(i uint64) *Message

	// Get size of buffer
	GetBufferSize() uint64

	// Get current index of Business Logic Consumer
	GetExecutorIndex() uint64
}

type InputOrbiter interface {
	// Set the receiver index to the given value
	SetReceiverIndex(uint64) error
	// Get current index of the receiver
	GetReceiverIndex() uint64

	// Set the journaler index to the given value
	SetJournalerIndex(uint64) error
	// Get current index of the journaler
	GetJournalerIndex() uint64

	// Set the replicator index to the given value
	SetReplicatorIndex(uint64) error
	// Get current index of the replicator
	GetReplicatorIndex() uint64

	// Set the unmarshaller index to the given value
	SetUnmarshallerIndex(uint64) error
	// Get current index of the unmarshaller
	GetUnmarshallerIndex() uint64

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

	// Business Logic Consumer
	executorIndex   uint64
	executorHandler Handler

	// Flag to indicate whether Orbiter is running
	running bool
}

// inputOrbiter unmarshals messages and coordinates journalling and replication.
type inputOrbiter struct {
	orbiter

	// Receiver
	receiverIndex   uint64
	receiverHandler Handler
	receiverBuffer  chan []byte

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

// NewInputOrbiter initializes a new inputOrbiter.
//
// All indexes are set to 0 and Handlers are assigned.
//
// Space for the buffer is allocated and is filled with empty Message objects.
//
// It returns a pointer to the initialized inputOrbiter.
func NewInputOrbiter(
	size uint64,
	receiver Handler,
	journaler Handler,
	replicator Handler,
	unmarshaller Handler,
	executor Handler,
) *inputOrbiter {
	orbiter := &inputOrbiter{
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

		// Create the channel for the receiverBuffer
		receiverBuffer: make(chan []byte, 4096),
	}
	// Start at index 4 so we don't have index underflow
	orbiter.Reset(4)

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
func (o *inputOrbiter) Reset(i uint64) error {
	if o.running {
		return errors.New("Cannot reset a running Orbiter")
	}

	// Bypass the setters otherwise their sanity checks will error
	//
	o.receiverIndex = i
	o.journalerIndex = i - 1
	o.replicatorIndex = i - 2
	o.unmarshallerIndex = i - 3
	o.executorIndex = i - 4

	return nil
}

// Start starts the Orbiter processing.
// It launches a number of goroutines (one for each Handler + one manager).
//
// These goroutines handle the index checking logic and call the provided
// Handler function when there is data in the buffer available for it to
// process.
func (o *inputOrbiter) Start() {
	go o.run()
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

// SetExecutorIndex sets the inputOrbiter's executorIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current unmarshallerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetExecutorIndex(i uint64) error {
	if i < o.GetExecutorIndex() {
		return errors.New("New executor index cannot be less than current " +
			"index")
	} else if i > o.GetUnmarshallerIndex()-1 {
		return errors.New("New executor index cannot be greater than the " +
			"current unmarshaller index")
	}

	o.executorIndex = i
	return nil
}

// GetExecutorIndex returns the Orbiter's current executorIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *orbiter) GetExecutorIndex() uint64 {
	return o.executorIndex
}

// SetReceiverindex sets the inputOrbiter's receiverIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current executorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetReceiverIndex(i uint64) error {
	if i < o.GetReceiverIndex() {
		return errors.New("New receiver index cannot be less than current " +
			"index")
	} else if i > o.GetExecutorIndex()-1 {
		return errors.New("New receiver index cannot be greater than the " +
			"current executor index")
	}

	o.receiverIndex = i
	return nil
}

// GetReceiverIndex returns the inputOrbiter's current receiverIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *inputOrbiter) GetReceiverIndex() uint64 {
	return o.receiverIndex
}

// SetJournalerIndex sets the inputOrbiter's journalerIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current receiverIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetJournalerIndex(i uint64) error {
	if i < o.GetJournalerIndex() {
		return errors.New("New journaler index cannot be less than current " +
			"index")
	} else if i > o.GetReceiverIndex()-1 {
		return errors.New("New journaler index cannot be greater than the " +
			"current receiver index")
	}

	o.journalerIndex = i
	return nil
}

// GetJournalerIndex returns the inputOrbiter's current journalerIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *inputOrbiter) GetJournalerIndex() uint64 {
	return o.journalerIndex
}

// SetReplicatorIndex sets the inputOrbiter's replicatorIndex to the given
// value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current journalerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetReplicatorIndex(i uint64) error {
	if i < o.GetReplicatorIndex() {
		return errors.New("New replicator index cannot be less than current " +
			"index")
	} else if i > o.GetJournalerIndex()-1 {
		return errors.New("New replicator index cannot be greater than the " +
			"current journaler index")
	}

	o.replicatorIndex = i
	return nil
}

// GetReplicatorIndex returns the inputOrbiter's current replicatorIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *inputOrbiter) GetReplicatorIndex() uint64 {
	return o.replicatorIndex
}

// SetUnmarshallerIndex sets the inputOrbiter's unmarshallerIndex to the given
// value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current replicatorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetUnmarshallerIndex(i uint64) error {
	if i < o.GetUnmarshallerIndex() {
		return errors.New("New unmarshaller index cannot be less than " +
			"current index")
	} else if i > o.GetReplicatorIndex()-1 {
		return errors.New("New unmarshaller index cannot be greater than the " +
			"current replicator index")
	}

	o.unmarshallerIndex = i
	return nil
}

// GetUnmarshallerIndex returns the inputOrbiter's current unmarshallerIndex.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
func (o *inputOrbiter) GetUnmarshallerIndex() uint64 {
	return o.unmarshallerIndex
}

// runOrbiter starts all the Handler management goroutines.
func (o *inputOrbiter) run() {
	o.running = true
	go o.runReceiver(o.receiverHandler)
	go o.runHandler(o.journalerHandler, "journaler")
	go o.runHandler(o.replicatorHandler, "replicator")
	go o.runHandler(o.unmarshallerHandler, "unmarshaller")
	go o.runHandler(o.executorHandler, "executor")
}

// runReceiver processes messages sent to it until the channel is closed.
func (o *inputOrbiter) runReceiver(h Handler) {
	var i uint64
	for msg := range o.receiverBuffer {
		i = o.GetReceiverIndex()
		o.buffer[i%o.GetBufferSize()] = &Message{
			id:         i,
			marshalled: msg,
		}
		h(o, []uint64{i})
	}
}

// runHandler loops, calling the Handler when Messages are available to process.
//
// TODO gracefully handle Orbiter.Stop(). Currently all handlers stop
// immediately. Better behaviour would be for Receiver to stop first and the
// rest of the handlers finish processing anything available to them before
// stopping. This could be achieved better by the Orbiter.Stop() function
// closing the channel buffer and the receiver exiting on no more data.
func (o *inputOrbiter) runHandler(h Handler, t string) {
	var this, last, i, j uint64
	for o.running {
		// Get the current indexes.
		// this - current index of this Handler
		// last - highest index that this Handler can process
		switch t {
		case "journaler":
			this = o.GetJournalerIndex()
			last = o.GetReceiverIndex() - 1
		case "replicator":
			this = o.GetReplicatorIndex()
			last = o.GetJournalerIndex() - 1
		case "unmarshaller":
			this = o.GetUnmarshallerIndex()
			last = o.GetReplicatorIndex() - 1
		case "executor":
			this = o.GetExecutorIndex()
			last = o.GetUnmarshallerIndex() - 1
		}

		// Check if we can process anything
		if this < last {
			// Build list of indexes to process
			indexes := make([]uint64, last-this)
			for i, j = this+1, 0; i <= last; i, j = i+1, j+1 {
				indexes[j] = i
			}

			// Call the Handler
			h(o, indexes)
		}
	}
}
