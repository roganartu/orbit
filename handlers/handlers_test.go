package handlers_test

import (
	"testing"

	"github.com/bmizerany/assert"

	"github.com/roganartu/orbitus/handlers"

	target "github.com/roganartu/orbitus"
)

const buffer_size uint64 = 256 // 2^8

func TestLoopSetIndex(t *testing.T) {
	loop := target.New(buffer_size, nil, nil, nil, nil, nil)
	var err error
	var old uint64

	// Setting to value lower than current index should fail
	old = loop.GetIndex(handlers.RECEIVER)
	err = loop.SetIndex(handlers.RECEIVER, loop.GetIndex(handlers.RECEIVER)-1)
	assert.Equal(t, "new index cannot be less than current index", err.Error())
	assert.Equal(t, old, loop.GetIndex(handlers.RECEIVER))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(handlers.RECEIVER)
	err = loop.SetIndex(handlers.RECEIVER, loop.GetIndex(handlers.RECEIVER))
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(handlers.RECEIVER))

	// Basic incrementing by one (where there is room in front) should work.
	old = loop.GetIndex(handlers.RECEIVER)
	err = loop.SetIndex(handlers.RECEIVER, old+1)
	assert.Equal(t, nil, err)
	assert.Equal(t, old+1, loop.GetIndex(handlers.RECEIVER))

	// Wrapping around should work, even though new modulus of index is lower
	// than the index of the consumer in front of it.
	loop.Reset(4)
	assert.Equal(t, uint64(0), loop.GetIndex(handlers.EXECUTOR))
	err = loop.SetIndex(handlers.RECEIVER, buffer_size)
	assert.Equal(t, nil, err)
	assert.Equal(t, buffer_size, loop.GetIndex(handlers.RECEIVER))

	// Wrapping around and setting index to the same location as the Business
	// Logic Consumer should not work.
	loop.Reset(4)
	assert.Equal(t, uint64(0), loop.GetIndex(handlers.EXECUTOR))
	old = loop.GetIndex(handlers.RECEIVER)
	err = loop.SetIndex(handlers.RECEIVER, buffer_size+1)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, "new index cannot overtake the index of the next consumer", err.Error())
	assert.Equal(t, old, loop.GetIndex(handlers.RECEIVER))

	// Wrapping around and passing the Business Logic Consumer should not work.
	loop.Reset(4)
	assert.Equal(t, uint64(0), loop.GetIndex(handlers.EXECUTOR))
	old = loop.GetIndex(handlers.RECEIVER)
	err = loop.SetIndex(handlers.RECEIVER, buffer_size+(buffer_size/2))
	assert.Equal(t, "new index cannot overtake the index of the next consumer", err.Error())
	assert.Equal(t, old, loop.GetIndex(handlers.RECEIVER))

	// Setting to same value as current should work (although do nothing really)
	old = loop.GetIndex(handlers.EXECUTOR)
	err = loop.SetIndex(handlers.EXECUTOR, old)
	assert.Equal(t, nil, err)
	assert.Equal(t, old, loop.GetIndex(handlers.EXECUTOR))
}
