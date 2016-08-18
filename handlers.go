package orbit

import (
	"errors"
)

const (
	RECEIVER = iota
	JOURNALER
	REPLICATOR
	UNMARSHALLER
	EXECUTOR
)

var (
	DEFAULT_HANDLER = [...]Handler{
		nil, // Required to simplify other logic around handler ordering
		defaultJournalerFunction,
		defaultReplicatorFunction,
		defaultUnmarshallerFunction,
		defaultExecutorFunction,
	}
)

// SetExecutorIndex sets the Loop's executorIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current unmarshallerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (l *Loop) SetExecutorIndex(i uint64) error {
	if i < l.GetIndex(EXECUTOR) {
		return errors.New("New executor index cannot be less than current " +
			"index")
	} else if i > l.GetIndex(UNMARSHALLER)-1 {
		return errors.New("New executor index cannot be greater than the " +
			"current unmarshaller index")
	}

	l.index[EXECUTOR] = i
	return nil
}

// SetReceiverIndex sets the Loop's receiverIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current executorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (l *Loop) SetReceiverIndex(i uint64) error {
	if i < l.GetIndex(RECEIVER) {
		return errors.New("New receiver index cannot be less than current " +
			"index")
	} else if i >= l.GetIndex(EXECUTOR)+l.GetBufferSize() {
		return errors.New("The Receiver Consumer cannot pass the Business " +
			"Logic Consumer")
	}

	l.index[RECEIVER] = i
	return nil
}

// SetJournalerIndex sets the Loop's journalerIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current receiverIndex.
//
// If the above rules are broken an error is returned, else nil.
func (l *Loop) SetJournalerIndex(i uint64) error {
	if i < l.GetIndex(JOURNALER) {
		return errors.New("New journaler index cannot be less than current " +
			"index")
	} else if i > l.GetIndex(RECEIVER)-1 {
		return errors.New("New journaler index cannot be greater than the " +
			"current receiver index")
	}

	l.index[JOURNALER] = i
	return nil
}

// SetReplicatorIndex sets the Loop's replicatorIndex to the given
// value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current journalerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (l *Loop) SetReplicatorIndex(i uint64) error {
	if i < l.GetIndex(REPLICATOR) {
		return errors.New("New replicator index cannot be less than current " +
			"index")
	} else if i > l.GetIndex(JOURNALER)-1 {
		return errors.New("New replicator index cannot be greater than the " +
			"current journaler index")
	}

	l.index[REPLICATOR] = i
	return nil
}

// SetUnmarshallerIndex sets the Loop's unmarshallerIndex to the given
// value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current replicatorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (l *Loop) SetUnmarshallerIndex(i uint64) error {
	if i < l.GetIndex(UNMARSHALLER) {
		return errors.New("New unmarshaller index cannot be less than " +
			"current index")
	} else if i > l.GetIndex(REPLICATOR)-1 {
		return errors.New("New unmarshaller index cannot be greater than the " +
			"current replicator index")
	}

	l.index[UNMARSHALLER] = i
	return nil
}

func defaultReceiverFunction(p Processor, id uint64, obj interface{}) {
	// Store message and current index
	elem := p.GetMessage(id)
	elem.SetID(id)

	if b, ok := obj.([]byte); ok {
		elem.SetMarshalled(b)
	} else {
		// Object isn't as expected but if we don't progress the buffer then the loop will hang.
		// Set the message to nil, future handlers should expect and handle this case.
		// TODO perhaps this should be an error instead?
		elem.SetMarshalled(nil)
	}

	p.SetMessage(id, elem)
	p.SetReceiverIndex(id + 1)
}

func defaultJournalerFunction(p Processor, ids []uint64) {
	p.SetJournalerIndex(ids[0])
}

func defaultReplicatorFunction(p Processor, ids []uint64) {
	p.SetReplicatorIndex(ids[0])
}

func defaultUnmarshallerFunction(p Processor, ids []uint64) {
	p.SetUnmarshallerIndex(ids[0])
}

func defaultExecutorFunction(p Processor, ids []uint64) {
	p.SetExecutorIndex(ids[0])
}
