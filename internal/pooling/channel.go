package pooling

import (
	"sync"
)

// DoneChan is a "done" channel used for signalling multiple goroutines by
// closing it.
type DoneChan chan struct{}

// ClosedDoneChan is a "pre-closed done channel" used whenever a new channel
// would be created just to close it immediately.
var ClosedDoneChan chan struct{}

// DoneChannels is a pool of chan struct{} intended to be used as "done"
// channels, similarly to the ctx.Done() in a context.
var DoneChannels doneChannel

type doneChannel sync.Pool

func init() {
	(*sync.Pool)(&DoneChannels).New = func() interface{} {
		return make(chan struct{})
	}

	ClosedDoneChan = make(chan struct{})
	close(ClosedDoneChan)
}

// Get returns an open, unbuffered channel.
func (p *doneChannel) Get() chan struct{} {
	return (*sync.Pool)(p).Get().(chan struct{})
}

// Put adds v to the pool if it's still open.
func (p *doneChannel) Put(v chan struct{}) {
	select {
	case _, ok := <-v:
		if !ok {
			(*sync.Pool)(p).Put(v)
		}
	default:
	}
}

// PutUnchecked adds v to the pool without checking that it's still open.
func (p *doneChannel) PutUnchecked(v chan struct{}) {
	(*sync.Pool)(p).Put(v)
}
