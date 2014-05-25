package orbitus

import (
	"errors"
)

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

// SetReceiverIndex sets the inputOrbiter's receiverIndex to the given value.
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
	} else if i >= o.GetExecutorIndex()+o.GetBufferSize() {
		return errors.New("The Receiver Consumer cannot pass the Business " +
			"Logic Consumer")
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
