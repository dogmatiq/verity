package infix

import (
	"reflect"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/api"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	"github.com/dogmatiq/marshalkit/codec/json"
	"github.com/dogmatiq/marshalkit/codec/protobuf"
)

// DefaultListenAddress is the default TCP address for the gRPC listener.
const DefaultListenAddress = ":50555"

// EngineOption configures the behavior of an engine.
type EngineOption func(*engineOptions)

// engineOptions is a container for a fully-resolved set of engine options.
type engineOptions struct {
	ListenAddress string
	Discoverer    api.Discoverer
	Marshaler     marshalkit.Marshaler
	Logger        logging.Logger
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

	if opts.Discoverer == nil {
		opts.Discoverer = api.NilDiscoverer
	}

	if opts.Marshaler == nil {
		opts.Marshaler = newMarshaler(cfg)
	}

	return opts
}

// newMarshaler returns the default marshaler to use for the given application.
func newMarshaler(cfg configkit.RichApplication) marshalkit.Marshaler {
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
