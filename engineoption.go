package verity

import (
	"fmt"
	"reflect"
	"runtime"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/enginekit/marshaler/codecs/json"
	"github.com/dogmatiq/enginekit/marshaler/codecs/protobuf"
	"github.com/dogmatiq/enginekit/marshaler/codecs/stateless"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/persistence/boltpersistence"
	"go.uber.org/zap"
)

var (
	// DefaultPersistenceProvider is the default persistence provider.
	//
	// It is overridden by the WithPersistence() option.
	DefaultPersistenceProvider persistence.Provider = &boltpersistence.FileProvider{
		Path: "/var/run/verity.boltdb",
	}

	// DefaultMessageTimeout is the default duration the engine allows for the
	// handling a single message by a Dogma handler.
	//
	// It is overridden by the WithMessageTimeout() option.
	DefaultMessageTimeout = 5 * time.Second

	// DefaultMessageBackoff is the default backoff strategy for message
	// handling retries.
	//
	// It is overridden by the WithMessageBackoff() option.
	DefaultMessageBackoff backoff.Strategy = backoff.WithTransforms(
		backoff.Exponential(100*time.Millisecond),
		linger.FullJitter,
		linger.Limiter(0, 1*time.Hour),
	)

	// DefaultConcurrencyLimit is the default number of messages to handle
	// (and projections to compact) concurrently.
	//
	// It is overridden by the WithConcurrencyLimit() option.
	DefaultConcurrencyLimit = uint(runtime.GOMAXPROCS(0) * 2)

	// DefaultProjectionCompactInterval is the default interval at which the
	// engine compacts projections.
	//
	// It is overridden by the WithProjectionCompactInterval() option.
	DefaultProjectionCompactInterval = 24 * time.Hour

	// DefaultProjectionCompactTimeout is the default timeout to use when
	// compacting a projection.
	//
	// It is overridden by the WithProjectionCompactTimeout() option.
	DefaultProjectionCompactTimeout = 5 * time.Minute

	// DefaultLogger is the default target for log messages produced by the
	// engine.
	//
	// It is overridden by the WithLogger() option.
	DefaultLogger = logging.DefaultLogger
)

// EngineOption configures the behavior of an engine.
type EngineOption func(*engineOptions)

// WithApplication returns an engine option that hosts an additional application
// on the engine.
//
// There must always be at least one application specified either by using
// WithApplication(), the app parameter to New(), or both.
func WithApplication(app dogma.Application) EngineOption {
	return func(opts *engineOptions) {
		cfg := configkit.FromApplication(app)

		for _, c := range opts.AppConfigs {
			if c.Identity().ConflictsWith(cfg.Identity()) {
				panic(fmt.Sprintf(
					"can not host both %s and %s because they have conflicting identities",
					c.Identity(),
					cfg.Identity(),
				))
			}
		}

		for _, h := range cfg.RichHandlers().Projections() {
			switch h.DeliveryPolicy().(type) {
			case dogma.UnicastProjectionDeliveryPolicy:
			default:
				panic(fmt.Sprintf(
					"the %s handler uses the %T delivery policy, which is not supported",
					h.Identity(),
					h.DeliveryPolicy(),
				))
			}
		}

		opts.AppConfigs = append(opts.AppConfigs, cfg)
	}
}

// WithPersistence returns an engine option that sets the persistence provider
// used to store and retrieve application state.
//
// If this option is omitted or p is nil, DefaultPersistenceProvider is used.
func WithPersistence(p persistence.Provider) EngineOption {
	return func(opts *engineOptions) {
		opts.PersistenceProvider = p
	}
}

// WithMessageTimeout returns an engine option that sets the duration the engine
// allows for the handling of a single message by a Dogma handler.
//
// If this option is omitted or d is zero DefaultMessageTimeout is used.
//
// Individual handler implementations within the application may provide timeout
// "hints", which the engine may by use instead of, or in conjunction with the
// duration specified by this option.
func WithMessageTimeout(d time.Duration) EngineOption {
	if d < 0 {
		panic("duration must not be negative")
	}

	return func(opts *engineOptions) {
		opts.MessageTimeout = d
	}
}

// WithMessageBackoff returns an engine option that sets the backoff strategy
// used to delay message handling retries.
//
// If this option is omitted or s is nil DefaultMessageBackoff is used.
func WithMessageBackoff(s backoff.Strategy) EngineOption {
	return func(opts *engineOptions) {
		opts.MessageBackoff = s
	}
}

// WithConcurrencyLimit returns an engine option that limits the number of
// messages that will be handled (and projections that will be compacted) at the
// same time.
//
// If this option is omitted or n non-positive DefaultConcurrencyLimit is used.
func WithConcurrencyLimit(n uint) EngineOption {
	return func(opts *engineOptions) {
		opts.ConcurrencyLimit = n
	}
}

// WithProjectionCompactInterval returns an engine option that sets the interval
// at which projections are compacted.
//
// If this option is omitted or d is zero DefaultProjectionCompactInterval is
// used.
func WithProjectionCompactInterval(d time.Duration) EngineOption {
	if d < 0 {
		panic("duration must not be negative")
	}

	return func(opts *engineOptions) {
		opts.ProjectionCompactInterval = d
	}
}

// WithProjectionCompactTimeout returns an engine option that sets the duration
// the engine allows for a single projection to be compacted.
//
// If this option is omitted or d is zero DefaultProjectionCompactTimeout is
// used.
func WithProjectionCompactTimeout(d time.Duration) EngineOption {
	if d < 0 {
		panic("duration must not be negative")
	}

	return func(opts *engineOptions) {
		opts.ProjectionCompactTimeout = d
	}
}

// NewDefaultMarshaler returns the default marshaler to use for the given
// applications.
//
// It is used if the WithMarshaler() option is omitted.
func NewDefaultMarshaler(configs []configkit.RichApplication) marshaler.Marshaler {
	var types []reflect.Type
	for _, cfg := range configs {
		for t := range cfg.MessageTypes() {
			types = append(types, t.ReflectType())
		}

		cfg.RichHandlers().RangeProcesses(
			func(h configkit.RichProcess) bool {
				r := h.Handler().New()
				types = append(types, reflect.TypeOf(r))
				return true
			},
		)
	}

	m, err := marshaler.New(
		types,
		[]marshaler.Codec{
			stateless.DefaultCodec,
			protobuf.DefaultNativeCodec,
			json.DefaultCodec,
		},
	)
	if err != nil {
		panic(err)
	}

	return m
}

// WithMarshaler returns a engine option that sets the marshaler used to marshal
// and unmarshal messages and other types.
//
// If this option is omitted or m is nil, NewDefaultMarshaler() is called to
// obtain the default marshaler.
func WithMarshaler(m marshaler.Marshaler) EngineOption {
	return func(opts *engineOptions) {
		opts.Marshaler = m
	}
}

// WithLogger returns an engine option that sets the target for log messages
// produced by the engine.
//
// l must be a [logging.Logger] or a [*zap.Logger].
//
// If this option is omitted or l is nil DefaultLogger is used.
func WithLogger(l any) EngineOption {
	return func(opts *engineOptions) {
		switch l := l.(type) {
		case *zap.Logger:
			opts.Logger = logging.Zap(l)
		case logging.Logger:
			opts.Logger = l
		case nil:
			opts.Logger = DefaultLogger
		default:
			panic(fmt.Sprintf(
				"unsupported logger type: %T",
				l,
			))
		}
	}
}

// engineOptions is a container for a fully-resolved set of engine options.
type engineOptions struct {
	AppConfigs                []configkit.RichApplication
	PersistenceProvider       persistence.Provider
	MessageTimeout            time.Duration
	MessageBackoff            backoff.Strategy
	ConcurrencyLimit          uint
	Marshaler                 marshaler.Marshaler
	ProjectionCompactInterval time.Duration
	ProjectionCompactTimeout  time.Duration
	Logger                    logging.Logger
	Network                   *networkOptions
}

// resolveEngineOptions returns a fully-populated set of engine options built from the
// given set of option functions.
func resolveEngineOptions(options ...EngineOption) *engineOptions {
	opts := &engineOptions{}

	for _, o := range options {
		o(opts)
	}

	if len(opts.AppConfigs) == 0 {
		panic("no applications configured, see verity.WithApplication()")
	}

	if opts.PersistenceProvider == nil {
		opts.PersistenceProvider = DefaultPersistenceProvider
	}

	if opts.MessageTimeout == 0 {
		opts.MessageTimeout = DefaultMessageTimeout
	}

	if opts.MessageBackoff == nil {
		opts.MessageBackoff = DefaultMessageBackoff
	}

	if opts.ConcurrencyLimit == 0 {
		opts.ConcurrencyLimit = DefaultConcurrencyLimit
	}

	if opts.ProjectionCompactInterval == 0 {
		opts.ProjectionCompactInterval = DefaultProjectionCompactInterval
	}

	if opts.ProjectionCompactTimeout == 0 {
		opts.ProjectionCompactTimeout = DefaultProjectionCompactTimeout
	}

	if opts.Marshaler == nil {
		opts.Marshaler = NewDefaultMarshaler(opts.AppConfigs)
	}

	if opts.Logger == nil {
		opts.Logger = DefaultLogger
	}

	return opts
}
