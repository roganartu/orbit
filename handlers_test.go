package orbit

import (
	"github.com/bmizerany/assert"
	"testing"
)

func TestLoopSetReceiverIndex(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = loop.GetIndex(RECEIVER)
	err = loop.SetReceiverIndex(loop.GetIndex(RECEIVER) - 1)
	assert.Equal(t, "New receiver index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, loop.GetIndex(RECEIVER))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(RECEIVER)
	err = loop.SetReceiverIndex(loop.GetIndex(RECEIVER))
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(RECEIVER))

	// Basic incrementing by one (where there is room in front) should work.
	old = loop.GetIndex(RECEIVER)
	err = loop.SetReceiverIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, loop.GetIndex(RECEIVER))

	// Wrapping around should work, even though new modulus of index is lower
	// than the index of the consumer in front of it.
	loop.Reset(5)
	assert.Equal(t, uint64(1), loop.GetIndex(EXECUTOR))
	err = loop.SetReceiverIndex(buffer_size)
	assert.Equal(t, nil, err)
	assert.Equal(t, buffer_size, loop.GetIndex(RECEIVER))

	// Wrapping around and setting index to the same location as the Business
	// Logic Consumer should not work.
	loop.Reset(5)
	assert.Equal(t, uint64(1), loop.GetIndex(EXECUTOR))
	old = loop.GetIndex(RECEIVER)
	err = loop.SetReceiverIndex(buffer_size + 1)
	assert.Equal(t, "The Receiver Consumer cannot pass the Business Logic "+
		"Consumer", err.Error())
	assert.Equal(t, old, loop.GetIndex(RECEIVER))

	// Wrapping around and passing the Business Logic Consumer should not work.
	loop.Reset(5)
	assert.Equal(t, uint64(1), loop.GetIndex(EXECUTOR))
	old = loop.GetIndex(RECEIVER)
	err = loop.SetReceiverIndex(buffer_size + (buffer_size / 2))
	assert.Equal(t, "The Receiver Consumer cannot pass the Business Logic "+
		"Consumer", err.Error())
	assert.Equal(t, old, loop.GetIndex(RECEIVER))
}

func TestLoopSetJournalerIndex(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = loop.GetIndex(JOURNALER)
	err = loop.SetJournalerIndex(loop.GetIndex(JOURNALER) - 1)
	assert.Equal(t, "New journaler index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, loop.GetIndex(JOURNALER))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(JOURNALER)
	err = loop.SetJournalerIndex(loop.GetIndex(JOURNALER))
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(JOURNALER))

	// Basic incrementing by one (where there is room in front) should work.
	loop.SetReceiverIndex(loop.GetIndex(RECEIVER) + 1)
	old = loop.GetIndex(JOURNALER)
	err = loop.SetJournalerIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, loop.GetIndex(JOURNALER))

	// Setting index to the same as the Receiver Consumer should not work.
	old = loop.GetIndex(JOURNALER)
	err = loop.SetJournalerIndex(loop.GetIndex(RECEIVER))
	assert.Equal(t, "New journaler index cannot be greater than the current "+
		"receiver index", err.Error())
	assert.Equal(t, old, loop.GetIndex(JOURNALER))

	// Passing the Receiver Consumer should not work
	old = loop.GetIndex(JOURNALER)
	err = loop.SetJournalerIndex(loop.GetIndex(RECEIVER) +
		loop.GetBufferSize() - 1)
	assert.Equal(t, "New journaler index cannot be greater than the current "+
		"receiver index", err.Error())
	assert.Equal(t, old, loop.GetIndex(JOURNALER))
}

func TestLoopSetReplicatorIndex(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = loop.GetIndex(REPLICATOR)
	err = loop.SetReplicatorIndex(old - 1)
	assert.Equal(t, "New replicator index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, loop.GetIndex(REPLICATOR))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(REPLICATOR)
	err = loop.SetReplicatorIndex(old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(REPLICATOR))

	// Basic incrementing by one (where there is room in front) should work.
	loop.SetReceiverIndex(loop.GetIndex(RECEIVER) + 1)
	loop.SetJournalerIndex(loop.GetIndex(JOURNALER) + 1)
	old = loop.GetIndex(REPLICATOR)
	err = loop.SetReplicatorIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, loop.GetIndex(REPLICATOR))

	// Setting index to the same as the Journaler Consumer should not work.
	old = loop.GetIndex(REPLICATOR)
	err = loop.SetReplicatorIndex(loop.GetIndex(JOURNALER))
	assert.Equal(t, "New replicator index cannot be greater than the current "+
		"journaler index", err.Error())
	assert.Equal(t, old, loop.GetIndex(REPLICATOR))

	// Passing the Journaler Consumer should not work
	old = loop.GetIndex(REPLICATOR)
	err = loop.SetReplicatorIndex(loop.GetIndex(JOURNALER) +
		loop.GetBufferSize() - 1)
	assert.Equal(t, "New replicator index cannot be greater than the current "+
		"journaler index", err.Error())
	assert.Equal(t, old, loop.GetIndex(REPLICATOR))
}

func TestLoopSetUnmarshallerIndex(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = loop.GetIndex(UNMARSHALLER)
	err = loop.SetUnmarshallerIndex(old - 1)
	assert.Equal(t, "New unmarshaller index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, loop.GetIndex(UNMARSHALLER))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(UNMARSHALLER)
	err = loop.SetUnmarshallerIndex(old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(UNMARSHALLER))

	// Basic incrementing by one (where there is room in front) should work.
	loop.SetReceiverIndex(loop.GetIndex(RECEIVER) + 1)
	loop.SetJournalerIndex(loop.GetIndex(JOURNALER) + 1)
	loop.SetReplicatorIndex(loop.GetIndex(REPLICATOR) + 1)
	old = loop.GetIndex(UNMARSHALLER)
	err = loop.SetUnmarshallerIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, loop.GetIndex(UNMARSHALLER))

	// Setting index to the same as the Replicator Consumer should not work.
	old = loop.GetIndex(UNMARSHALLER)
	err = loop.SetUnmarshallerIndex(loop.GetIndex(REPLICATOR))
	assert.Equal(t, "New unmarshaller index cannot be greater than the current "+
		"replicator index", err.Error())
	assert.Equal(t, old, loop.GetIndex(UNMARSHALLER))

	// Passing the Replicator Consumer should not work
	old = loop.GetIndex(UNMARSHALLER)
	err = loop.SetUnmarshallerIndex(loop.GetIndex(REPLICATOR) +
		loop.GetBufferSize() - 1)
	assert.Equal(t, "New unmarshaller index cannot be greater than the current "+
		"replicator index", err.Error())
	assert.Equal(t, old, loop.GetIndex(UNMARSHALLER))
}

func TestLoopSetExecutorIndex(t *testing.T) {
	loop := New(buffer_size, nil, nil, nil, nil, nil)
	loop.Reset(5)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = loop.GetIndex(EXECUTOR)
	err = loop.SetExecutorIndex(old - 1)
	assert.Equal(t, "New executor index cannot be less than current index",
		err.Error())
	assert.Equal(t, old, loop.GetIndex(EXECUTOR))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(EXECUTOR)
	err = loop.SetExecutorIndex(old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(EXECUTOR))

	// Basic incrementing by one (where there is room in front) should work.
	loop.SetReceiverIndex(loop.GetIndex(RECEIVER) + 1)
	loop.SetJournalerIndex(loop.GetIndex(JOURNALER) + 1)
	loop.SetReplicatorIndex(loop.GetIndex(REPLICATOR) + 1)
	loop.SetUnmarshallerIndex(loop.GetIndex(UNMARSHALLER) + 1)
	old = loop.GetIndex(EXECUTOR)
	err = loop.SetExecutorIndex(old + 1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, loop.GetIndex(EXECUTOR))

	// Setting index to the same as the Unmarshaller Consumer should not work.
	old = loop.GetIndex(EXECUTOR)
	err = loop.SetExecutorIndex(loop.GetIndex(UNMARSHALLER))
	assert.Equal(t, "New executor index cannot be greater than the current "+
		"unmarshaller index", err.Error())
	assert.Equal(t, old, loop.GetIndex(EXECUTOR))

	// Passing the Unmarshaller Consumer should not work
	old = loop.GetIndex(EXECUTOR)
	err = loop.SetExecutorIndex(loop.GetIndex(UNMARSHALLER) +
		loop.GetBufferSize() - 1)
	assert.Equal(t, "New executor index cannot be greater than the current "+
		"unmarshaller index", err.Error())
	assert.Equal(t, old, loop.GetIndex(EXECUTOR))
}
