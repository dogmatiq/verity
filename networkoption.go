package infix

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
	"google.golang.org/grpc"
)

var (
	// DefaultListenAddress is the default TCP address for the gRPC listener.
	//
	// It is overridden by the WithListenAddress() option.
	DefaultListenAddress = ":50555"

	// DefaultDialer is the default dialer used to connect to other engine's
	// gRPC servers.
	//
	// It is overridden by the WithDialer() option.
	DefaultDialer = discovery.DefaultDialer

	// DefaultDialerBackoff is the default backoff strategy for gRPC dialer
	// retries.
	//
	// It is overridden by the WithDialerBackoff() option.
	DefaultDialerBackoff backoff.Strategy = backoff.WithTransforms(
		backoff.Exponential(100*time.Millisecond),
		linger.FullJitter,
		linger.Limiter(0, 30*time.Second),
	)

	// DefaultDiscoverer is the default discoverer used to find
	// other engine instances on the network.
	//
	// It is overridden by the WithDiscoverer() option.
	DefaultDiscoverer = func(context.Context, discovery.TargetObserver) error {
		// TODO: https://github.com/dogmatiq/configkit/issues/58
		panic("no API discovery configured, see infix.WithDiscoverer()")
	}
)

// NetworkOption configures the networking-related behavior of an engine.
type NetworkOption func(*networkOptions)

// WithNetworking returns an engine option that enables network communication
// between engine instances running different Dogma applications.
//
// Engine instances communicate using gRPC APIs.
func WithNetworking(options ...NetworkOption) EngineOption {
	n := resolveNetworkOptions(options...)

	return func(opts *engineOptions) {
		opts.Network = n
	}
}

// WithListenAddress returns a network option that sets the TCP address for the
// engine's gRPC listener.
//
// If this option is omitted or addr is empty, DefaultListenAddress is used.
func WithListenAddress(addr string) NetworkOption {
	if addr != "" {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			panic(fmt.Sprintf("invalid listen address: %s", err))
		}

		if _, err := net.LookupPort("tcp", port); err != nil {
			panic(fmt.Sprintf("invalid listen address: %s", err))
		}
	}

	return func(opts *networkOptions) {
		opts.ListenAddress = addr
	}
}

// WithServerOptions returns a network option that adds gRPC server options.
func WithServerOptions(options ...grpc.ServerOption) NetworkOption {
	return func(opts *networkOptions) {
		opts.ServerOptions = append(opts.ServerOptions, options...)
	}
}

// WithDialer returns a network option that sets the dialer used to connect to
// other engine's gRPC servers.
//
// If this option is omitted or d is nil, DefaultDialer is used.
func WithDialer(d discovery.Dialer) NetworkOption {
	return func(opts *networkOptions) {
		opts.Dialer = d
	}
}

// WithDialerBackoff returns a network option that sets the backoff strategy
// used to delay gRPC dialing retries.
//
// If this option is omitted or s is nil, DefaultDialerBackoff is used.
func WithDialerBackoff(s backoff.Strategy) NetworkOption {
	return func(opts *networkOptions) {
		opts.DialerBackoff = s
	}
}

// Discoverer is a function that notifies an observer when a config API target
// becomes available or unavailable.
//
// It blocks until ctx is canceled or a fatal error occurs.
type Discoverer func(ctx context.Context, o discovery.TargetObserver) error

// WithDiscoverer returns a network option that sets the discoverer used to find
// other engine instances on the network.
//
// Currently this option MUST be specified.
//
// TODO: Use Bonjour as the default discovery mechanism. See
// See https://github.com/dogmatiq/configkit/issues/58
func WithDiscoverer(d Discoverer) NetworkOption {
	return func(opts *networkOptions) {
		opts.Discoverer = d
	}
}

// networkOptions is a container for a fully-resolve set of networking options.
type networkOptions struct {
	ListenAddress string
	ServerOptions []grpc.ServerOption
	Dialer        discovery.Dialer
	DialerBackoff backoff.Strategy
	Discoverer    Discoverer
}

// resolveNetworkOptions returns a fully-populated set of network options built
// from the given set of option functions.
func resolveNetworkOptions(options ...NetworkOption) *networkOptions {
	opts := &networkOptions{}

	for _, o := range options {
		o(opts)
	}

	if opts.ListenAddress == "" {
		opts.ListenAddress = DefaultListenAddress
	}

	if opts.Dialer == nil {
		opts.Dialer = DefaultDialer
	}

	if opts.DialerBackoff == nil {
		opts.DialerBackoff = DefaultDialerBackoff
	}

	if opts.Discoverer == nil {
		opts.Discoverer = DefaultDiscoverer
	}

	return opts
}
