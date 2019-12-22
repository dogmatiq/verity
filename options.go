package infix

import (
	"reflect"

	"github.com/dogmatiq/marshalkit/codec/protobuf"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/api"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
)

// DefaultListenAddress is the default TCP address for the gRPC listener.
const DefaultListenAddress = "*:50555"

// EngineOption configures the behavior of an engine.
type EngineOption func(*engineOptions)

// engineOptions is a
type engineOptions struct {
	ListenAddress string
	Discoverer    api.Discoverer
	Marshaler     marshalkit.Marshaler
}

// resolveOptions returns a fully-populated set of engine options built from the
// given set of option functions.
func resolveOptions(
	cfg configkit.RichApplication,
	options []EngineOption,
) (*engineOptions, error) {
	opts := &engineOptions{}

	for _, o := range options {
		o(opts)
	}

	if opts.Marshaler == nil {
		m, err := newMarshaler(cfg)
		if err != nil {
			return nil, err
		}
		opts.Marshaler = m
	}

	if opts.Discoverer == nil {
		opts.Discoverer = &api.StaticDiscoverer{}
	}

	if opts.ListenAddress == "" {
		opts.ListenAddress = DefaultListenAddress
	}

	return opts, nil
}

// newMarshaler returns the default marshaler to use for the given application.
func newMarshaler(cfg configkit.RichApplication) (marshalkit.Marshaler, error) {
	var types []reflect.Type
	for t := range cfg.MessageTypes().All() {
		types = append(types, t.ReflectType())
	}

	return codec.NewMarshaler(
		types,
		[]codec.Codec{
			&protobuf.NativeCodec{},
		},
	)
}
