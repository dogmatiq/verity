package api

import (
	"sync"

	"google.golang.org/grpc"
)

// Registry is a collection of gRPC clients.
//
// It implements the Discoverer interface, allowing subscribers to observe as
// clients are added and removed from the registry.
type Registry struct {
	m           sync.Mutex
	clients     map[*grpc.ClientConn]struct{}
	subscribers map[Subscriber]struct{}
}

// Update updates the registry to contain only the given list of clients.
func (r *Registry) Update(clients []*grpc.ClientConn) {
	r.m.Lock()
	defer r.m.Unlock()

	prev := r.clients
	r.clients = make(map[*grpc.ClientConn]struct{}, len(clients))

	for _, c := range clients {
		r.clients[c] = struct{}{}

		if _, ok := prev[c]; !ok {
			for sub := range r.subscribers {
				sub.Discovered(c)
			}
		}
	}

	for c := range prev {
		if _, ok := r.clients[c]; !ok {
			for sub := range r.subscribers {
				sub.Undiscovered(c)
			}
		}
	}
}

// Add adds the given client to the registry.
func (r *Registry) Add(c *grpc.ClientConn) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.clients[c]; ok {
		return
	}

	if r.clients == nil {
		r.clients = map[*grpc.ClientConn]struct{}{}
	}

	r.clients[c] = struct{}{}

	for sub := range r.subscribers {
		sub.Discovered(c)
	}
}

// Remove removes the given client from the registry.
func (r *Registry) Remove(c *grpc.ClientConn) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.clients[c]; !ok {
		return
	}

	delete(r.clients, c)

	for sub := range r.subscribers {
		sub.Undiscovered(c)
	}
}

// Subscribe registers a subscriber with the discoverer, causing it to be
// notified of any changes to the set of known remote APIs.
func (r *Registry) Subscribe(s Subscriber) {
	r.m.Lock()
	defer r.m.Unlock()

	if _, ok := r.subscribers[s]; ok {
		return
	}

	if r.subscribers == nil {
		r.subscribers = map[Subscriber]struct{}{}
	}

	r.subscribers[s] = struct{}{}

	for c := range r.clients {
		s.Discovered(c)
	}
}

// Unsubscribe removes a subscriber from the discoverer, stopping it from
// being notified of any changes to the set of known remote APIs.
func (r *Registry) Unsubscribe(s Subscriber) {
	r.m.Lock()
	defer r.m.Unlock()

	delete(r.subscribers, s)
}
