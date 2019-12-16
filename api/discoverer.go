package api

import (
	"context"

	"google.golang.org/grpc"
)

// Discoverer is an interface for discovering API endpoints of engines running
// other Dogma applications.
type Discoverer interface {
	// Discover locates the API endpoints of engines running other Dogma
	// applications.
	Discover(ctx context.Context) ([]*grpc.ClientConn, error)
}

// StaticDiscoverer is a discoverer that discovers API endpoints from a fixed
// list of network addresses.
type StaticDiscoverer struct {
	Hosts   []string
	Options []grpc.DialOption
}

// Discover locates the API endpoints of engines running other Dogma
// applications.
func (d *StaticDiscoverer) Discover(ctx context.Context) ([]*grpc.ClientConn, error) {
	conns := make([]*grpc.ClientConn, len(d.Hosts))

	for i, host := range d.Hosts {
		conn, err := grpc.DialContext(ctx, host, d.Options...)
		if err == nil {
			conns[i] = conn
		}
	}

	return conns, nil
}
