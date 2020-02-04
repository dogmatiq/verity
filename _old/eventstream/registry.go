package eventstream

import (
	"sync"
)

// Registry is a collection of event streams.
//
// It implements the Publisher interface, allowing observers to be notified as
// streams are added to and removed from the registry.
type Registry struct {
	m         sync.Mutex
	streams   map[Stream]struct{}
	observers map[Observer]struct{}
}

// Add adds the given stream to the registry.
func (r *Registry) Add(s Stream) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.streams[s]; ok {
		return
	}

	if r.streams == nil {
		r.streams = map[Stream]struct{}{}
	}

	r.streams[s] = struct{}{}

	for o := range r.observers {
		o.StreamAvailable(s)
	}
}

// Remove removes the given client from the registry.
func (r *Registry) Remove(s Stream) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.streams[s]; !ok {
		return
	}

	delete(r.streams, s)

	for o := range r.observers {
		o.StreamUnavailable(s)
	}
}

// RegisterStreamObserver registers an observer to be notified of updates to
// stream availability.
//
// The observer is immediately notified of the available streams.
func (r *Registry) RegisterStreamObserver(o Observer) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.observers[o]; ok {
		return
	}

	if r.observers == nil {
		r.observers = map[Observer]struct{}{}
	}

	r.observers[o] = struct{}{}

	for s := range r.streams {
		o.StreamAvailable(s)
	}
}

// UnregisterStreamObserver stops an observer from being notified of updates
// to stream availability.
func (r *Registry) UnregisterStreamObserver(o Observer) {
	r.m.Lock()
	defer r.m.Unlock()

	delete(r.observers, o)
}
