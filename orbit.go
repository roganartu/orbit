// Orbit provides an implementation of the LMAX Disruptor.
//
// For more info on LMAX see Martin Fowler's blog post: http://martinfowler.com/articles/lmax.html
//
// Alternatively read the paper: http://disruptor.googlecode.com/files/Disruptor-1.0.pdf
package orbit

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

// This is the minimum number of GOMAXPROCS that is required to run this lib
// from an external thread (eg: the calling thread) without deadlocking.
// There's 5 orbit threads (main loop + 4 handlers), plus 1 for the calling thread.
// TODO allow users to configure which handlers they are ok with not being locked?
// This will degrade performance, but would be interesting to benchmark how much,
// especially if eg: the main receiving thread isn't blocked since everything
// else can happily happen async.
const minProcs = 6

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

// ReceiverHandler is the same as Handler except it takes arbitrary input
// and stores it in the buffer instead of processing data that is already in the buffer.
// TODO merge this with Handler by passing in a struct with a bool to store, and a start/end
// index instead of a slice?
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
	// SetIndex sets the index at the given pos to the given value.
	SetIndex(int, uint64) error

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
	handlers ...Handler,
) *Loop {
	loop := &Loop{
		bufferSize: size,
		buffer:     make([]unsafe.Pointer, size),

		// Start in stopped state
		running:       false,
		startStopLock: &sync.Mutex{},

		// Create handler and index arrays
		handler: make([]Handler, len(handlers)),
		index:   make([]uint64, len(handlers)+1),
		channel: make([]chan struct{}, len(handlers)+1),

		// Create the input stream channel
		// TODO allow tweaking this size. It is likely to affect performance, especially in IO-heavy workloads
		Input: make(chan interface{}, 4096),
	}
	// Assign Handlers
	loop.receiver = receiver
	for k, v := range handlers {
		loop.handler[k] = v
	}

	// Start at the last index so we don't underflow as we add new items
	loop.Reset(uint64(len(handlers)))

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

// SetIndex sets the index of the given handler id to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the next,
// after wrapping around
//
// If the above rules are broken an error is returned, else nil.
func (l *Loop) SetIndex(pos int, val uint64) error {
	if val < l.GetIndex(pos) {
		return errors.New("new index cannot be less than current index")
	}

	nextIndex := l.GetNextIndex(pos)
	if val >= nextIndex {
		// pos=0 is a special case here, because GetNextIndex wraps around backwards.
		// To handle this we need to check if the new index value will wrap around
		// past the last consumer in the buffer.
		if pos != 0 || (val > l.GetBufferSize() && val%l.GetBufferSize() > nextIndex) {
			return errors.New("new index cannot overtake the index of the next consumer")
		}
	}

	l.index[pos] = val
	return nil
}

// Reset sets all indexes to a given value.
// This is useful for rebuilding Loop state from an input file
// (eg: journaled output) instead of manually looping through the buffer until
// the desired index is reached.
//
// Returns an error if called while Lopp is running. Stop Loop with
// Loop.Stop() before resetting.
func (l *Loop) Reset(i uint64) error {
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	if l.running {
		return errors.New("cannot reset a running Loop")
	}

	// Bypass the setters otherwise their sanity checks will error
	// This organises the handlers in reverse order in the ring buffer.
	// eg if i==4:
	//     4: Receiver
	//     3: Journaler
	//     2: Replicator
	//     1: Unmarshaller
	//     0: Executor
	for j, _ := range l.index {
		// The first item in index should be first in the buffer
		l.index[j] = i - uint64(j)
	}

	return nil
}

// Start starts the Loop processing.
// It launches a number of goroutines (one for each Handler + one manager).
//
// These goroutines handle the index checking logic and call the provided
// Handler function when there is data in the buffer available for it to
// process.
func (l *Loop) Start() error {
	// if runtime.GOMAXPROCS(-1) < minProcs {
	// 	return errors.New("GOMAXPROCS is insufficient to lock all handlers to separate threads")
	// }

	// Allocate channels
	for i := range l.channel {
		l.channel[i] = make(chan struct{}, 1)
	}

	// TODO add config to enable CPU and mem profiling
	go l.run()
	return nil
}

// Stop stops the Loop processing.
// This stops processing gracefully. All messages sent to l.Input before calling Stop
// will be processed.
func (l *Loop) Stop() {
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	l.running = false
	// TODO register this as an OnStop event
	// Stop the receiver channel so we don't process any new messages.
	if l.channel[0] != nil {
		close(l.channel[0])
	}
}

// GetIndex returns the Loop's current index for the provided Consumer.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
//
// h is the handler to fetch the index for.
func (l *Loop) GetIndex(h int) uint64 {
	// The 0th index is the receiver
	return l.index[h]
}

// GetNextIndex returns the Loop's current index for the Consumer after the provided pos.
// This is the same as GetIndex, except it returns the index at h-1 (wrapping
// around to the end of the pos list if h is the first item).
//
// This is in reverse order, because of the way the ring buffer is arranged.
// The receiver is always at the front of the ring buffer, and lives at h=0.
// All other consumers are arranged in order of execution in the pipeline after
// the Receiver, ie:
//    0: Receiver
//    1: First consumer in pipeline
//    2: 2nd consumer in pipeline
//    3: Last consumer in pipeline
//
// h is the handler to fetch the index for.
func (l *Loop) GetNextIndex(h int) uint64 {
	if h-1 < 0 {
		return l.index[len(l.index)-1]
	}
	return l.index[h-1]
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

	// TODO Make these run on different OS threads with runtime.LockOSThread
	// TODO actually allow these to be configured, instead of always using the defaults.
	go l.runReceiver(l.receiver)
	for i, handler := range l.handler {
		go l.runHandler(handler, i+1)
	}
}

// runReceiver processes messages sent to it until the channel is closed.
func (l *Loop) runReceiver(h ReceiverHandler) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var i uint64
	firstChannel := l.channel[1]
	for msg := range l.Input {
		i = l.GetIndex(0)

		// Run handler
		if h != nil {
			h(l, i, msg)
		}

		// Let the next handler know it can proceed without blocking this one
		select {
		case firstChannel <- struct{}{}:
			// journal handler was notified
		default:
			// journal handler already knows, but hasn't processed new messages yet
		}
	}

	// Close the other handler channels so they stop gracefully
	l.startStopLock.Lock()
	defer l.startStopLock.Unlock()
	for k, v := range l.channel {
		if k != 0 {
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
	nextChannel := l.channel[(t+1)%(len(l.channel))]
	for _ = range l.channel[t] {
	retry:
		// Get the current indexes.
		// this - current index of this Handler
		// last - highest index that this Handler can process
		this = l.GetIndex(t)
		last = l.GetNextIndex(t) - 1

		// Check if we can process anything, sleep and retry if not
		if this >= last {
			// Wait until the other handlers have processed things
			// TODO make this sleep configurable
			time.Sleep(100 * time.Microsecond)
			goto retry
		}

		// Build list of indexes to process
		// TODO limit this to some max buffer size?
		indexes := make([]uint64, last-this)
		for i, j = this+1, 0; i <= last; i, j = i+1, j+1 {
			indexes[j] = i
		}

		// Call the Handler
		if h != nil {
			h(l, indexes)
		}

		// Let the next handler know it can proceed without blocking this one
		if t < len(l.handler) {
			select {
			case nextChannel <- struct{}{}:
				// Notified next handler that Messages are available
			default:
				// Handler already knows, but hasn't processed new messages yet
			}
		}
	}
}
