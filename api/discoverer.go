package api

import (
	"context"

	"google.golang.org/grpc"
)

// Discoverer is an interface for discovering API endpoints of engines running
// other Dogma applications.
type Discoverer interface {
	// Subscribe registers a subscriber with the discoverer, causing it to be
	// notified of any changes to the set of known remote APIs.
	Subscribe(DiscoverySubscriber)

	// Unsubscribe removes a subscriber from the discoverer, stopping it from
	// being notified of any changes to the set of known remote APIs.
	Unsubscribe(DiscoverySubscriber)
}

// DiscoverySubscriber is an interface that is notified by a discoverer when a
// remote API is "discovered" or "undiscovered".
type DiscoverySubscriber interface {
	// Discovered is called by a discover when a remote API is discovered.
	Discovered(*grpc.ClientConn)

	// Undiscovered is called by a discover when a remote API is undiscovered.
	Undiscovered(*grpc.ClientConn)
}

// NilDiscoverer is a discoverer that does not discover any API endpoints.
var NilDiscoverer Discoverer = staticDiscoverer{}

// staticDiscoverer is a discoverer that notifies subscribers of a fixed
// list of gRPC clients.
type staticDiscoverer []*grpc.ClientConn

// NewStaticDiscoverer returns a discoverer that notifies subscribers of a fixed
// list of gRPC clients.
func NewStaticDiscoverer(
	ctx context.Context,
	hosts []string,
	options ...grpc.DialOption,
) (Discoverer, error) {
	conns := make(staticDiscoverer, len(hosts))

	for i, h := range hosts {
		conn, err := grpc.DialContext(ctx, h, options...)
		if err == nil {
			conns[i] = conn
		}
	}

	return conns, nil
}

// Subscribe registers a subscriber with the discoverer, causing it to be
// notified of any changes to the set of known remote APIs.
func (d staticDiscoverer) Subscribe(s DiscoverySubscriber) {
	for _, c := range d {
		s.Discovered(c)
	}
}

// Unsubscribe removes a subscriber from the discoverer, stopping it from
// being notified of any changes to the set of known remote APIs.
func (d staticDiscoverer) Unsubscribe(DiscoverySubscriber) {
}
