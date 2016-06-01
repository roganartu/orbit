package orbitus

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

// SetExecutorIndex sets the Orbiter's executorIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current unmarshallerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *Orbiter) SetExecutorIndex(i uint64) error {
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

// SetReceiverIndex sets the Orbiter's receiverIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current executorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *Orbiter) SetReceiverIndex(i uint64) error {
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

// SetJournalerIndex sets the Orbiter's journalerIndex to the given value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current receiverIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *Orbiter) SetJournalerIndex(i uint64) error {
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

// SetReplicatorIndex sets the Orbiter's replicatorIndex to the given
// value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current journalerIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *Orbiter) SetReplicatorIndex(i uint64) error {
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

// SetUnmarshallerIndex sets the Orbiter's unmarshallerIndex to the given
// value.
//
// The provided value is checked to ensure that it is within acceptable bounds.
// Specifically, it cannot be less than the current index or greater than the
// current replicatorIndex.
//
// If the above rules are broken an error is returned, else nil.
func (o *Orbiter) SetUnmarshallerIndex(i uint64) error {
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

func defaultReceiverFunction(p Processor, id uint64, obj interface{}) {
	// Store message and current index
	elem := p.GetMessage(id)
	elem.SetID(id)

	if b, ok := obj.([]byte); ok {
		elem.SetMarshalled(b)
	} else {
		// Object isn't as expected, don't progress the buffer
		return
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
