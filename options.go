package infix

import (
	"reflect"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	"github.com/dogmatiq/marshalkit/codec/json"
	"github.com/dogmatiq/marshalkit/codec/protobuf"
	"google.golang.org/grpc"
)

// EngineOption configures the behavior of an engine.
type EngineOption func(*engineOptions)

// DefaultMessageBackoffStrategy is the default message backoff strategy.
var DefaultMessageBackoffStrategy backoff.Strategy = backoff.WithTransforms(
	backoff.Exponential(100*time.Millisecond),
	linger.FullJitter,
	linger.Limiter(0, 1*time.Hour),
)

// WithMessageBackoffStrategy returns an option that sets the strategy used to
// determine when the engine should retry a message after a failure.
//
// If this option is omitted or s is nil DefaultBackoffStrategy is used.
func WithMessageBackoffStrategy(s backoff.Strategy) EngineOption {
	return func(opts *engineOptions) {
		opts.MessageBackoffStrategy = s
	}
}

// DefaultMessageTimeout is the default timeout to apply when handling a
// message.
const DefaultMessageTimeout = 5 * time.Second

// WithMessageTimeout returns an option that sets the default timeout applied
// when handling a message.
//
// The default is only used if the specific message handler does not provide a
// timeout hint.
//
// If this option is omitted or d is zero DefaultMessageTimeout is used.
func WithMessageTimeout(d time.Duration) EngineOption {
	if d < 0 {
		panic("duration must not be negative")
	}

	return func(opts *engineOptions) {
		opts.MessageTimeout = d
	}
}

// NewDefaultMarshaler returns the default marshaler to use for the given
// application configuration.
func NewDefaultMarshaler(cfg configkit.RichApplication) marshalkit.Marshaler {
	var types []reflect.Type
	for t := range cfg.MessageTypes().All() {
		types = append(types, t.ReflectType())
	}

	m, err := codec.NewMarshaler(
		types,
		[]codec.Codec{
			&protobuf.NativeCodec{},
			&json.Codec{},
		},
	)
	if err != nil {
		panic(err)
	}

	return m
}

// WithMarshaler returns an option that sets the marshaler used to marshal and
// unmarshal messages and other types.
//
// If this option is omitted or m is nil NewDefaultMarshaler() is called to
// obtain the default marshaler.
func WithMarshaler(m marshalkit.Marshaler) EngineOption {
	return func(opts *engineOptions) {
		opts.Marshaler = m
	}
}

// DefaultLogger is the default target for log messages produced by the engine.
var DefaultLogger = logging.DefaultLogger

// WithLogger returns an option that sets the target for log messages produced
// by the engine.
//
// If this option is omitted or l is nil DefaultLogger is used.
func WithLogger(l logging.Logger) EngineOption {
	return func(opts *engineOptions) {
		opts.Logger = l
	}
}

// engineOptions is a container for a fully-resolved set of engine options.
type engineOptions struct {
	ListenAddress          string
	MessageBackoffStrategy backoff.Strategy
	MessageTimeout         time.Duration
	Discoverer             Discoverer
	Dialer                 discovery.Dialer
	DialerBackoffStrategy  backoff.Strategy
	ServerOptions          []grpc.ServerOption
	Marshaler              marshalkit.Marshaler
	Logger                 logging.Logger
}

// resolveOptions returns a fully-populated set of engine options built from the
// given set of option functions.
func resolveOptions(
	cfg configkit.RichApplication,
	options []EngineOption,
) *engineOptions {
	opts := &engineOptions{}

	for _, o := range options {
		o(opts)
	}

	if opts.ListenAddress == "" {
		opts.ListenAddress = DefaultListenAddress
	}

	if opts.MessageBackoffStrategy == nil {
		opts.MessageBackoffStrategy = DefaultMessageBackoffStrategy
	}

	if opts.MessageTimeout == 0 {
		opts.MessageTimeout = DefaultMessageTimeout
	}

	if opts.Dialer == nil {
		opts.Dialer = DefaultDialer
	} else if opts.Discoverer == nil {
		panic("WithDialer() can not be used without WithDiscoverer()")
	}

	if opts.DialerBackoffStrategy == nil {
		opts.DialerBackoffStrategy = DefaultDialerBackoffStrategy
	}

	if opts.Marshaler == nil {
		opts.Marshaler = NewDefaultMarshaler(cfg)
	}

	if opts.Logger == nil {
		opts.Logger = DefaultLogger
	}

	return opts
}
