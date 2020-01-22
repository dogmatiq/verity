package eventstream

import (
	"context"
	"sync"

	"github.com/dogmatiq/configkit"
)

// Supervisor starts a goroutine for each discovered event stream.
type Supervisor struct {
	// Func is the entry-point for the goroutines started for each stream.
	// The implementation must return when the supplied context is canceled.
	Func func(context.Context, Stream)

	// Parent is the parent context for the ctx argument passed to Func.
	Parent context.Context

	m       sync.Mutex
	cancels map[configkit.Identity]*context.CancelFunc // pointer is used to allow comparison
}

// Discovered is called by a discoverer when an event stream is discovered.
func (s *Supervisor) Discovered(str Stream) {
	s.m.Lock()
	defer s.m.Unlock()

	s.stop(str)
	s.start(str)
}

// Undiscovered is called by a discoverer when an event stream is undiscovered.
func (s *Supervisor) Undiscovered(str Stream) {
	s.m.Lock()
	defer s.m.Unlock()

	s.stop(str)
}

// start launches a goroutine for the given stream.
// It assumes that s.m has already been locked.
func (s *Supervisor) start(str Stream) {
	id := str.Application()

	ctx, cancel := context.WithCancel(s.Parent)
	ptr := &cancel
	s.cancels[id] = ptr

	// GOROUTINE EXIT STRATEGY: The goroutine ends when s.Func() returns. A
	// properly behaving s.Func() *MUST* return when its context is canceled.
	go func() {
		defer s.removeIf(str, ptr)
		s.Func(ctx, str)
	}()
}

// stop cancels the context associated with the goroutine for the given stream.
// It assumes that s.m has already been locked.
func (s *Supervisor) stop(str Stream) {
	id := str.Application()

	if cancel, ok := s.cancels[id]; ok {
		(*cancel)()
	}
}

// removeIf removes the cancellation function for str from s.cancels only if it
// is equal to ptr.
//
// It assumes that s.m HAS NOT been locked.
func (s *Supervisor) removeIf(str Stream, ptr *context.CancelFunc) {
	id := str.Application()

	s.m.Lock()
	defer s.m.Unlock()

	// note: This comparison is the reason we use a pointer-to-function,
	// rather than the function directly.
	if s.cancels[id] == ptr {
		delete(s.cancels, id)
	}
}
