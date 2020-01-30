package api

import (
	"google.golang.org/grpc"
)

// Discoverer is an interface for discovering API endpoints of engines running
// other Dogma applications.
type Discoverer interface {
	// Subscribe registers a subscriber with the discoverer, causing it to be
	// notified of any changes to the set of known remote APIs.
	Subscribe(Subscriber)

	// Unsubscribe removes a subscriber from the discoverer, stopping it from
	// being notified of any changes to the set of known remote APIs.
	Unsubscribe(Subscriber)
}

// Subscriber is an interface that is notified by a discoverer when a remote API
// is "discovered" or "undiscovered".
type Subscriber interface {
	// Discovered is called by a discover when a remote API is discovered.
	Discovered(*grpc.ClientConn)

	// Undiscovered is called by a discover when a remote API is undiscovered.
	Undiscovered(*grpc.ClientConn)
}
