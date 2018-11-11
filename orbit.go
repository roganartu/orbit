// Orbit provides an implementation of the LMAX Disruptor.
//
// For more info on LMAX see Martin Fowler's blog post: http://martinfowler.com/articles/lmax.html
//
// Alternatively read the paper: http://disruptor.googlecode.com/files/Disruptor-1.0.pdf
package orbit

import (
	"errors"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Handler is the Consumer handler function type.
// Defaults are provided, however it is expected most users will define their
// own functions. It will be called by the Loops when new messages are
// available for processing. It is not expected to be a long-running process.
//
// It should remember the index of the last item it has processed. It is the
// Handler's responsibility to update the corresponding index via the
// appropriate Loop methods once processing of a Message is complete.
//
// It is assumed that all objects between the index stored in the Loop
// and the next consumer (backwards). Once this index is set the consumer behind
// will be allowed to process up to an including the new value.
// The second parameter is an array of indexes for the handler to process.
//
// If the consumers current index is equal to the index of the consumer ahead of
// it the consumer should wait until this changes before processing any
// messages. This state should only really ever happen on startup or reset, but
// it is safe practice for the system in any case.
type Handler func(Processor, []uint64)

// ReceiverHandler is the same as Handler except it is designed to take arbitrary input
// and store it in the buffer instead of processing data that is already in the buffer.
type ReceiverHandler func(Processor, uint64, interface{})

// Loop maintains buffers and Consumers.
//
// Consumers are defined in the order they will be positined in the ring buffer.
// If the Loop is intended to receive data from a client the Executor handler
// should perform business logic. If the Loop is intended to send responses to
// clients then the Executor handler should write to a HTTP response.
type Loop struct {
	// Buffered interface channel for the input stream
	Input chan interface{}

	// Receiver handler. Similar to Handler but accepts interface
	receiver ReceiverHandler

	// The circular buffer
	buffer []unsafe.Pointer

	// Size of the buffer
	// Must be a power of 2
	bufferSize uint64

	// Indexes, handlers and channels for all consumers
	index   []uint64
	handler []Handler
	channel []chan struct{}

	// Flag to indicate whether Loop is running
	running       bool
	startStopLock *sync.Mutex
}

// Processor marshals/unmarshals Business Logic Consumer output received from and sent to clients.
//
// The typical use case is as follows:
//     Client ->
//     Receiver (Receiver -> Journaler -> Replicator -> Unmarshaller -> Executor) ->
//     Sender (Receiver -> Journaler -> Replicator -> Marshaller -> Publisher) ->
//     Client
// Strictly speaking the Sender is not required and is mostly useful for using Orbit
// behind a client endpoint where the client expects a response.
type Processor interface {
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

	// Get the message at the given address in the buffer.
	GetMessage(uint64) *Message

	// Set the message at the given address in the buffer.
	SetMessage(uint64, *Message)
}

// New initializes a new Loop
//
// All indexes are set to the beginning of the buffer and Handlers are assigned.
// Space for the buffer is allocated and is filled with empty Message objects.
func New(
	size uint64,
	receiver ReceiverHandler,
	journaler Handler,
	replicator Handler,
	unmarshaller Handler,
	executor Handler,
) *Loop {
	loop := &Loop{
		bufferSize: size,
		buffer:     make([]unsafe.Pointer, size),

		// Start in stopped state
		running:       false,
		startStopLock: &sync.Mutex{},

		// Create handler and index arrays
		handler: make([]Handler, 5),
		index:   make([]uint64, 5),
		channel: make([]chan struct{}, 5),

		// Create the input stream channel
		// TODO allow tweaking this size. It is likely to affect performance, especially in IO-heavy workloads
		Input: make(chan interface{}, 4096),
	}
	// Assign Handlers
	loop.receiver = receiver
	loop.handler[JOURNALER] = journaler
	loop.handler[REPLICATOR] = replicator
	loop.handler[UNMARSHALLER] = unmarshaller
	loop.handler[EXECUTOR] = executor

	if loop.receiver == nil {
		loop.receiver = defaultReceiverFunction
	}
	for k, v := range loop.handler {
		if k != RECEIVER && v == nil {
			loop.handler[k] = DEFAULT_HANDLER[k]
		}
	}

	// Start at index 4 so we don't have index underflow
	loop.Reset(4)

	// Create 'size' new Message objects and store them in the buffer.
	// This avoids costly object creation and GC while streaming data.
	var i uint64
	for i = 0; i < size; i++ {
		msg := &Message{}
		msg.Init()
		// Just needs to be anything, so that the atomic.StorePointer doesn't panic on nil deref
		loop.buffer[i] = unsafe.Pointer(&struct{}{})
		atomic.StorePointer(&loop.buffer[i], unsafe.Pointer(msg))
	}

	return loop
}

// Reset sets all indexes to a given value.
// This is useful for rebuilding Loop state from an input file
// (eg: journaled output) instead of manually looping through the buffer until
// the desired index is reached.
//
// Returns an error if called while Lopp is running. Stop Loop with
// Loop.Stop() before resetting.
func (l *Loop) Reset(i uint64) error {
	if l.running {
		return errors.New("Cannot reset a running Loop")
	}

	// Bypass the setters otherwise their sanity checks will error
	var j uint64
	for j = 0; j <= EXECUTOR; j++ {
		// The first item in index should be first in the buffer
		l.index[j] = i - j
	}

	return nil
}

// Start starts the Loop processing.
// It launches a number of goroutines (one for each Handler + one manager).
//
// These goroutines handle the index checking logic and call the provided
// Handler function when there is data in the buffer available for it to
// process.
func (l *Loop) Start() {
	// Allocate channels
	for i := range l.channel {
		l.channel[i] = make(chan struct{}, 1)
	}

	go l.run()
}

// Stop stops the Loop processing.
// This stops processing gracefully. All messages sent to l.Input before calling Stop
// will be processed.
func (l *Loop) Stop() {
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	l.running = false
	close(l.channel[RECEIVER])
}

// GetIndex returns the Loop's current index for the provided Consumer.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
//
// h is the handler to fetch the index for.
func (l *Loop) GetIndex(h int) uint64 {
	return l.index[h]
}

// GetMessage returns the message at the given address in the buffer.
//
// If the provided index is larger than the buffer size then the modulus is
// used to generate an index that is in range.
func (l *Loop) GetMessage(i uint64) *Message {
	// Bounds check
	if i >= l.bufferSize {
		i = i % l.bufferSize
	}

	return (*Message)(atomic.LoadPointer(&l.buffer[i]))
}

// SetMessage sets the message at the given address in the buffer.
//
// If the provided index is larger than the buffer size then the modulus is
// used to generate an index that is in range.
//
// The message is not copied, do not hold a reference to it after setting it with this method.
func (l *Loop) SetMessage(i uint64, m *Message) {
	// Bounds check
	if i >= l.bufferSize {
		i = i % l.bufferSize
	}

	atomic.StorePointer(&l.buffer[i], unsafe.Pointer(m))
}

// GetBufferSize returns the size of the Loop's Message buffer array.
func (l *Loop) GetBufferSize() uint64 {
	return l.bufferSize
}

// run starts all the Handler management goroutines.
func (l *Loop) run() {
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	l.running = true

	go l.runReceiver(l.receiver)
	go l.runHandler(l.handler[JOURNALER], JOURNALER)
	go l.runHandler(l.handler[REPLICATOR], REPLICATOR)
	go l.runHandler(l.handler[UNMARSHALLER], UNMARSHALLER)
	go l.runHandler(l.handler[EXECUTOR], EXECUTOR)
}

// runReceiver processes messages sent to it until the channel is closed.
func (l *Loop) runReceiver(h ReceiverHandler) {
	var i uint64
	journalChannel := l.channel[RECEIVER+1]
	arr := []uint64{0}
	for msg := range l.Input {
		i = l.GetIndex(RECEIVER)

		// Run handler
		if h != nil {
			arr[0] = i
			h(l, i, msg)
		}

		// Let the next handler know it can proceed without blocking this one
		select {
		case journalChannel <- struct{}{}:
			// journal handler was notified
		default:
			// journal handler already knows, but hasn't processed new messages yet
		}
	}

	// Close the other handler channels so they stop gracefully
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	for k, v := range l.channel {
		if k != RECEIVER {
			close(v)
		}
	}
}

// runHandler loops, calling the Handler when Messages are available to process.
//
// Gracefully handles Loop.Stop(). runReceiver stops first and the rest of
// handlers finish processing anything available to them before stopping.
func (l *Loop) runHandler(h Handler, t int) {
	var this, last, i, j uint64
	nextChannel := l.channel[(t+1)%(EXECUTOR+1)]
	for _ = range l.channel[t] {
		// Get the current indexes.
		// this - current index of this Handler
		// last - highest index that this Handler can process
		this = l.GetIndex(t)
		last = l.GetIndex(t-1) - 1

		// Check if we can process anything
		if this < last {
			// Build list of indexes to process
			indexes := make([]uint64, last-this)
			for i, j = this+1, 0; i <= last; i, j = i+1, j+1 {
				indexes[j] = i
			}

			// Call the Handler
			if h != nil {
				h(l, indexes)
			}

			// Let the next handler know it can proceed without blocking this one
			if t != EXECUTOR {
				select {
				case nextChannel <- struct{}{}:
					// Notified next handler that Messages are available
				default:
					// Handler already knows, but hasn't processed new messages yet
				}
			}
		}
	}
}
