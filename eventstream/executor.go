package eventstream

import (
	"context"

	"github.com/dogmatiq/configkit"
)

// Executor starts a goroutine for each discovered event stream.
type Executor struct {
	Parent  context.Context
	Func    func(context.Context, Stream)
	cancels map[configkit.Identity]func()
}

// Discovered is called by a discover when an event stream is discovered.
func (e *Executor) Discovered(s Stream) {
	id := s.Application()

	if cancel, ok := e.cancels[id]; ok {
		cancel()
	}

	ctx, cancel := context.WithCancel(e.Parent)
	e.cancels[id] = cancel

	go e.Func(ctx, s)
}

// Undiscovered is called by a discover when an event stream is undiscovered.
func (e *Executor) Undiscovered(s Stream) {
	id := s.Application()

	if cancel, ok := e.cancels[id]; ok {
		cancel()
		delete(e.cancels, id)
	}
}
