package infix

import (
	"reflect"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/provider/boltdb"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	"github.com/dogmatiq/marshalkit/codec/json"
	"github.com/dogmatiq/marshalkit/codec/protobuf"
)

// DefaultPersistenceProvider is the default persistence provider.
var DefaultPersistenceProvider persistence.Provider = &boltdb.Provider{}

// WithPersistenceProvider returns an option that sets the persistence provider
// used to store and retreive application state.
//
// If this option is omitted or p is nil, DefaultPersistenceProvider is used.
func WithPersistenceProvider(p persistence.Provider) EngineOption {
	return func(opts *engineOptions) {
		opts.PersistenceProvider = p
	}
}

// NewDefaultMarshaler returns the default marshaler to use for the given
// application configuration.
func NewDefaultMarshaler(configs []configkit.RichApplication) marshalkit.Marshaler {
	var types []reflect.Type
	for _, cfg := range configs {
		for t := range cfg.MessageTypes().All() {
			types = append(types, t.ReflectType())
		}
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
// If this option is omitted or m is nil, NewDefaultMarshaler() is called to
// obtain the default marshaler.
func WithMarshaler(m marshalkit.Marshaler) EngineOption {
	return func(opts *engineOptions) {
		opts.Marshaler = m
	}
}
