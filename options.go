package infix

import (
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	"github.com/dogmatiq/marshalkit/codec/json"
	"github.com/dogmatiq/marshalkit/codec/protobuf"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/trace"
)

// EngineOption configures the behavior of an engine.
type EngineOption func(*engineOptions)

// DefaultListenAddress is the default TCP address for the gRPC listener.
const DefaultListenAddress = ":50555"

// ListenAddress returns an option that sets the TCP address for the gRPC
// listener.
//
// If this option is omitted or addr is empty DefaultListenAddress is used.
func ListenAddress(addr string) EngineOption {
	if addr != "" {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			panic(fmt.Sprintf("invalid listen address: %s", err))
		}

		if _, err := net.LookupPort("tcp", port); err != nil {
			panic(fmt.Sprintf("invalid listen address: %s", err))
		}
	}

	return func(opts *engineOptions) {
		opts.ListenAddress = addr
	}
}

// DefaultMessageTimeout is the default timeout to apply when handling a message.
const DefaultMessageTimeout = 5 * time.Second

// MessageTimeout returns an option that sets the default timeout applied when
// handling a message.
//
// The default is only used if the specific message handler does not provide a
// timeout hint.
//
// If this option is omitted or d is zero DefaultMessageTimeout is used.
func MessageTimeout(d time.Duration) EngineOption {
	if d < 0 {
		panic("duration must not be negative")
	}

	return func(opts *engineOptions) {
		opts.MessageTimeout = d
	}
}

// DefaultRetryPolicy is the default message retry policy.
var DefaultRetryPolicy handler.RetryPolicy = handler.ExponentialBackoff{
	Min:    100 * time.Millisecond,
	Max:    1 * time.Hour,
	Jitter: 0.25,
}

// RetryPolicy returns an option that sets the retry policy used to determine
// when the engine should retry a message after a failure.
//
// If this option is omitted or p is nil DefaultMessagePolicy is used.
func RetryPolicy(p handler.RetryPolicy) EngineOption {
	return func(opts *engineOptions) {
		opts.RetryPolicy = p
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

// Marshaler returns an option that sets the marshaler used to marshal and
// unmarshal messages and other types.
//
// If this option is omitted or m is nil NewDefaultMarshaler() is called to
// obtain the default marshaler.
func Marshaler(m marshalkit.Marshaler) EngineOption {
	return func(opts *engineOptions) {
		opts.Marshaler = m
	}
}

// DefaultLogger is the default target for log messages produced by the engine.
var DefaultLogger = logging.DefaultLogger

// Logger returns an option that sets the target for log messages produced by
// the engine.
//
// If this option is omitted or l is nil DefaultLogger is used.
func Logger(l logging.Logger) EngineOption {
	return func(opts *engineOptions) {
		opts.Logger = l
	}
}

// DefaultMeter is the default OpenTelemetry meter used to record metrics.
var DefaultMeter = metric.NoopMeter{}

// Meter returns an option that sets the OpenTelemetry meter used to record
// metrics throughout the engine.
//
// If this option is omitted or m is nil, DefaultMeter is used.
func Meter(m metric.Meter) EngineOption {
	return func(opts *engineOptions) {
		opts.Meter = m
	}
}

// DefaultTracer is the default OpenTelemetry tracer used to record spans.
var DefaultTracer = trace.NoopTracer{}

// Tracer returns an option that sets the OpenTelemetry tracer used to record
// spans throughout the engine.
//
// If this option is omitted or t is nil, DefaultTraver is used.
func Tracer(t trace.Tracer) EngineOption {
	return func(opts *engineOptions) {
		opts.Tracer = t
	}
}

// engineOptions is a container for a fully-resolved set of engine options.
type engineOptions struct {
	ListenAddress  string
	MessageTimeout time.Duration
	RetryPolicy    handler.RetryPolicy
	Marshaler      marshalkit.Marshaler
	Logger         logging.Logger
	Meter          metric.Meter
	Tracer         trace.Tracer
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

	if opts.MessageTimeout == 0 {
		opts.MessageTimeout = DefaultMessageTimeout
	}

	if opts.RetryPolicy == nil {
		opts.RetryPolicy = DefaultRetryPolicy
	}

	if opts.Marshaler == nil {
		opts.Marshaler = NewDefaultMarshaler(cfg)
	}

	if opts.Logger == nil {
		opts.Logger = DefaultLogger
	}

	if opts.Meter == nil {
		opts.Meter = DefaultMeter
	}

	if opts.Tracer == nil {
		opts.Tracer = DefaultTracer
	}

	return opts
}
