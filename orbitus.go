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
type Handler func(Receiver, []uint64)

// Orbiter maintains buffers and Consumers.
//
// Consumers are defined in the order they will be positined in the ring buffer.
// If the orbiter is intended to receive data from a client the Executor handler
// should perform business logic. If the orbiter is intended to send responses to
// clients then the Executor handler should write to a HTTP response.
type Orbiter struct {
	// Buffered byte channel for the input stream
	Input chan []byte

	// The circular buffer
	buffer []*Message

	// Size of the buffer
	// Must be a power of 2
	bufferSize uint64

	// Indexes, handlers and channels for all consumers
	index   []uint64
	handler []Handler
	channel []chan int

	// Flag to indicate whether Orbiter is running
	running bool
}

// Processor marshals/unmarshals Business Logic Consumer output received from and sent to clients.
//
// The typical use case is as follows:
//     Client ->
//     Receiver (Receiver -> Journaler -> Replicator -> Unmarshaller -> Executor) ->
//     Sender (Receiver -> Journaler -> Replicator -> Marshaller -> Publisher) ->
//     Client
// Strictly speaking the Sender is not required and is mostly useful for using Orbitus
// behind a client endpoint where the client expects a response.
type Receiver interface {
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

// NewOrbiter initializes a new Orbiter
//
// All indexes are set to the beginning of the buffer and Handlers are assigned.
// Space for the buffer is allocated and is filled with empty Message objects.
func NewOrbiter(
	size uint64,
	receiver Handler,
	journaler Handler,
	replicator Handler,
	unmarshaller Handler,
	executor Handler,
) *Orbiter {
	orbiter := &Orbiter{
		bufferSize: size,
		buffer:     make([]*Message, size),

		// Start in stopped state
		running: false,

		// Create handler and index arrays
		handler: make([]Handler, 5),
		index:   make([]uint64, 5),
		channel: make([]chan int, 5),

		// Create the input stream channel
		// TODO allow tweaking this size. It is likely to affect performance, especially in IO-heavy workloads
		Input: make(chan []byte, 4096),
	}
	// Assign Handlers
	orbiter.handler[RECEIVER] = receiver
	orbiter.handler[JOURNALER] = journaler
	orbiter.handler[REPLICATOR] = replicator
	orbiter.handler[UNMARSHALLER] = unmarshaller
	orbiter.handler[EXECUTOR] = executor
	for k, v := range orbiter.handler {
		if v == nil {
			orbiter.handler[k] = DEFAULT_HANDLER[k]
		}
	}

	// Start at index 4 so we don't have index underflow
	orbiter.Reset(4)

	// Create 'size' new Message objects and store them in the buffer.
	// This avoids costly object creation and GC while streaming data.
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
func (o *Orbiter) Reset(i uint64) error {
	if o.running {
		return errors.New("Cannot reset a running Orbiter")
	}

	// Bypass the setters otherwise their sanity checks will error
	var j uint64
	for j = 0; j <= EXECUTOR; j++ {
		// The first item in index should be first in the buffer
		o.index[j] = i - j
	}

	return nil
}

// Start starts the Orbiter processing.
// It launches a number of goroutines (one for each Handler + one manager).
//
// These goroutines handle the index checking logic and call the provided
// Handler function when there is data in the buffer available for it to
// process.
func (o *Orbiter) Start() {
	// Allocate channels
	for i := range o.channel {
		o.channel[i] = make(chan int, 1)
	}

	go o.run()
}

// Stop stops the Orbiter processing.
// This stops processing gracefully. All messages sent to o.Input before calling Stop
// will be processed.
func (o *Orbiter) Stop() {
	o.running = false
	close(o.channel[RECEIVER])
}

// GetIndex returns the Orbiter's current index for the provided Consumer.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
//
// h is the handler to fetch the index for.
func (o *Orbiter) GetIndex(h int) uint64 {
	return o.index[h]
}

// GetMessage returns the message at the given address in the buffer.
//
// If the provided index is larger than the buffer size then the modulus is
// used to generate an index that is in range.
func (o *Orbiter) GetMessage(i uint64) *Message {
	// Bounds check
	if i >= o.bufferSize {
		i = i % o.bufferSize
	}

	return o.buffer[i]
}

// GetBufferSize returns the size of the Orbiter's Message buffer array.
func (o *Orbiter) GetBufferSize() uint64 {
	return o.bufferSize
}

// runOrbiter starts all the Handler management goroutines.
func (o *Orbiter) run() {
	o.running = true
	go o.runReceiver(o.handler[RECEIVER])
	go o.runHandler(o.handler[JOURNALER], JOURNALER)
	go o.runHandler(o.handler[REPLICATOR], REPLICATOR)
	go o.runHandler(o.handler[UNMARSHALLER], UNMARSHALLER)
	go o.runHandler(o.handler[EXECUTOR], EXECUTOR)
}

// runReceiver processes messages sent to it until the channel is closed.
func (o *Orbiter) runReceiver(h Handler) {
	var i uint64
	journalChannel := o.channel[RECEIVER+1]
	arr := []uint64{0}
	for msg := range o.Input {
		i = o.GetIndex(RECEIVER)

		// Store message and current index
		elem := o.buffer[i%o.GetBufferSize()]
		elem.id = i
		elem.marshalled = msg

		// Run handler
		if h != nil {
			arr[0] = i
			h(o, arr)
		}

		// Let the next handler know it can proceed
		if len(journalChannel) == 0 {
			journalChannel <- 1
		}
	}

	// Close the other handler channels so they stop gracefully
	for k, v := range o.channel {
		if k != RECEIVER {
			close(v)
		}
	}
}

// runHandler loops, calling the Handler when Messages are available to process.
//
// Gracefully handles Orbiter.Stop(). runReceiver stops first and the rest of
// handlers finish processing anything available to them before stopping.
func (o *Orbiter) runHandler(h Handler, t int) {
	var this, last, i, j uint64
	nextChannel := o.channel[(t+1)%(EXECUTOR+1)]
	for _ = range o.channel[t] {
		// Get the current indexes.
		// this - current index of this Handler
		// last - highest index that this Handler can process
		this = o.GetIndex(t)
		last = o.GetIndex(t-1) - 1

		// Check if we can process anything
		if this < last {
			// Build list of indexes to process
			indexes := make([]uint64, last-this)
			for i, j = this+1, 0; i <= last; i, j = i+1, j+1 {
				indexes[j] = i
			}

			// Call the Handler
			if h != nil {
				h(o, indexes)
			}

			// Let the next handler know it can proceed without blocking this one
			if len(nextChannel) == 0 && o.running && t != EXECUTOR {
				select {
				case nextChannel <- 1:
					// Notified next handler that Messages are available
				default:
					// Handler already knows, but hasn't processed new messages yet
				}
			}
		}
	}
}
