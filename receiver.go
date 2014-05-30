package orbitus

import (
	"errors"
)

const (
	RECEIVER     = iota
	JOURNALER    = iota
	REPLICATOR   = iota
	UNMARSHALLER = iota
	EXECUTOR     = iota
)

// inputOrbiter unmarshals messages and coordinates journalling and replication.
type inputOrbiter struct {
	orbiter

	// Receiver message buffer
	receiverBuffer chan []byte
}

// NewReceiverOrbiter initializes a new inputOrbiter.
//
// All indexes are set to 0 and Handlers are assigned.
//
// Space for the buffer is allocated and is filled with empty Message objects.
//
// It returns a pointer to the initialized inputOrbiter.
func NewReceiverOrbiter(
	size uint64,
	receiver Handler,
	journaler Handler,
	replicator Handler,
	unmarshaller Handler,
	executor Handler,
) *inputOrbiter {
	orbiter := &inputOrbiter{
		// Allocate the buffer
		orbiter: orbiter{
			buffer_size: size,
			buffer:      make([]*Message, size),

			// Start in stopped state
			running: false,

			// Create handler and index arrays
			handler: make([]Handler, 5),
			index:   make([]uint64, 5),
			channel: make([]chan int, 5),
		},

		// Create the channel for the receiverBuffer
		receiverBuffer: make(chan []byte, 4096),
	}
	// Assign Handlers
	orbiter.handler[RECEIVER] = receiver
	orbiter.handler[JOURNALER] = journaler
	orbiter.handler[REPLICATOR] = replicator
	orbiter.handler[UNMARSHALLER] = unmarshaller
	orbiter.handler[EXECUTOR] = executor

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
func (o *inputOrbiter) Start() {
	// Allocate channels
	for i := range o.channel {
		o.channel[i] = make(chan int)
	}

	go o.run()
}

// Stop stops the Orbiter processing.
// It closes the input stream and then closes all channels, effectively
// killing all goroutines.
func (o *inputOrbiter) Stop() {
	o.running = false
	for i := 0; i <= EXECUTOR; i++ {
		close(o.channel[i])
	}
}

// GetIndex returns the inputOrbiter's current index for the provided Consumer.
// This index may be larger than the buffer size, as the modulus is used to get
// a valid array index.
//
// h is the handler to fetch the index for.
func (o *inputOrbiter) GetIndex(h int) uint64 {
	return o.index[h]
}

// SetExecutorIndex sets the inputOrbiter's executorIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current unmarshallerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetExecutorIndex(i uint64) error {
	if i < o.GetIndex(EXECUTOR) {
		return errors.New("New executor index cannot be less than current " +
			"index")
	} else if i > o.GetIndex(UNMARSHALLER)-1 {
		return errors.New("New executor index cannot be greater than the " +
			"current unmarshaller index")
	}

	o.index[EXECUTOR] = i
	return nil
}

// SetReceiverIndex sets the inputOrbiter's receiverIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current executorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetReceiverIndex(i uint64) error {
	if i < o.GetIndex(RECEIVER) {
		return errors.New("New receiver index cannot be less than current " +
			"index")
	} else if i >= o.GetIndex(EXECUTOR)+o.GetBufferSize() {
		return errors.New("The Receiver Consumer cannot pass the Business " +
			"Logic Consumer")
	}

	o.index[RECEIVER] = i
	return nil
}

// SetJournalerIndex sets the inputOrbiter's journalerIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current receiverIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *inputOrbiter) SetJournalerIndex(i uint64) error {
	if i < o.GetIndex(JOURNALER) {
		return errors.New("New journaler index cannot be less than current " +
			"index")
	} else if i > o.GetIndex(RECEIVER)-1 {
		return errors.New("New journaler index cannot be greater than the " +
			"current receiver index")
	}

	o.index[JOURNALER] = i
	return nil
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
	if i < o.GetIndex(REPLICATOR) {
		return errors.New("New replicator index cannot be less than current " +
			"index")
	} else if i > o.GetIndex(JOURNALER)-1 {
		return errors.New("New replicator index cannot be greater than the " +
			"current journaler index")
	}

	o.index[REPLICATOR] = i
	return nil
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
	if i < o.GetIndex(UNMARSHALLER) {
		return errors.New("New unmarshaller index cannot be less than " +
			"current index")
	} else if i > o.GetIndex(REPLICATOR)-1 {
		return errors.New("New unmarshaller index cannot be greater than the " +
			"current replicator index")
	}

	o.index[UNMARSHALLER] = i
	return nil
}

// runOrbiter starts all the Handler management goroutines.
func (o *inputOrbiter) run() {
	o.running = true
	go o.runReceiver(o.handler[RECEIVER])
	go o.runHandler(o.handler[JOURNALER], JOURNALER)
	go o.runHandler(o.handler[REPLICATOR], REPLICATOR)
	go o.runHandler(o.handler[UNMARSHALLER], UNMARSHALLER)
	go o.runHandler(o.handler[EXECUTOR], EXECUTOR)
}

// runReceiver processes messages sent to it until the channel is closed.
func (o *inputOrbiter) runReceiver(h Handler) {
	var i uint64
	journalChannel := o.channel[RECEIVER+1]
	for msg := range o.receiverBuffer {
		i = o.GetIndex(RECEIVER)

		// Store message and current index
		elem := o.buffer[i%o.GetBufferSize()]
		elem.id = i
		elem.marshalled = msg

		// Run handler
		if h != nil {
			h(o, []uint64{i})
		}

		// Let the next handler know it can proceed
		if len(journalChannel) == 0 {
			journalChannel <- 1
		}
	}
}

// runHandler loops, calling the Handler when Messages are available to process.
//
// TODO gracefully handle Orbiter.Stop(). Currently all handlers stop
// immediately. Better behaviour would be for Receiver to stop first and the
// rest of the handlers finish processing anything available to them before
// stopping. This could be achieved better by the Orbiter.Stop() function
// closing the channel buffer and the receiver exiting on no more data.
func (o *inputOrbiter) runHandler(h Handler, t int) {
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

			// Let the next handler know it can proceed
			if len(nextChannel) == 0 && o.running && t != EXECUTOR {
				nextChannel <- 1
			}
		}
	}
}
