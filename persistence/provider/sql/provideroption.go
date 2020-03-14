package sql

import (
	"runtime"
	"time"

	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/linger/backoff"
)

var (
	// DefaultMaxIdleConnections is the default maximum number of idle
	// connections allowed in the database pool.
	//
	// It is overridden by the WithMaxIdleConnections() option.
	DefaultMaxIdleConnections = runtime.GOMAXPROCS(0)

	// DefaultMaxOpenConnections is the default maximum number of open
	// connections allowed in the database pool.
	//
	// It is overridden by the WithMaxOpenConnections() option.
	DefaultMaxOpenConnections = DefaultMaxIdleConnections * 10

	// DefaultMaxLifetime is the default maximum lifetime of database
	// connections.
	//
	// It is overridden by the WithMaxLifetime() option.
	DefaultMaxLifetime = 10 * time.Minute

	// DefaultStreamBackoff is the default backoff strategy for stream polling.
	//
	// It is overridden by the WithStreamBackoff() option.
	DefaultStreamBackoff backoff.Strategy = backoff.WithTransforms(
		backoff.Exponential(10*time.Millisecond),
		linger.FullJitter,
		linger.Limiter(0, 5*time.Second),
	)
)

// ProviderOption configures the behavior of an SQL provider.
type ProviderOption func(*providerOptions)

// WithMaxIdleConnections returns a provider option that sets the maximum number
// of idle connections allowed in the database pool.
//
// If this option is omitted or n is zero, DefaultMaxIdleConnections is used.
func WithMaxIdleConnections(n int) ProviderOption {
	return func(opts *providerOptions) {
		opts.MaxIdle = n
	}
}

// WithMaxOpenConnections returns a provider option that sets the maximum number
// of open connections allowed in the database pool.
//
// If this option is omitted or n is zero, DefaultMaxOpenConnections is used.
func WithMaxOpenConnections(n int) ProviderOption {
	return func(opts *providerOptions) {
		opts.MaxOpen = n
	}
}

// WithMaxLifetime returns a provider option that sets the maximum lifetime of
// database connections.
//
// If this option is omitted or d is zero, DefaultMaxLifetime is used.
func WithMaxLifetime(d time.Duration) ProviderOption {
	return func(opts *providerOptions) {
		opts.MaxLifetime = d
	}
}

// WithStreamBackoff returns a provider option that sets the backoff strategy
// used to delay stream polls that produce no results.
//
// If this option is omitted or s is nil DefaultStreamBackoff is used.
func WithStreamBackoff(s backoff.Strategy) ProviderOption {
	return func(opts *providerOptions) {
		opts.StreamBackoff = s
	}
}

// providerOptions is a container for a fully-resolved set of provider options.
type providerOptions struct {
	MaxIdle       int
	MaxOpen       int
	MaxLifetime   time.Duration
	StreamBackoff backoff.Strategy
}

// resolveProviderOptions returns a fully-populated set of provider options
// built from the given set of option functions.
func resolveProviderOptions(options ...ProviderOption) *providerOptions {
	opts := &providerOptions{}

	for _, o := range options {
		o(opts)
	}

	if opts.MaxIdle == 0 {
		opts.MaxIdle = DefaultMaxIdleConnections
	}

	if opts.MaxOpen == 0 {
		opts.MaxOpen = DefaultMaxOpenConnections
	}

	if opts.MaxLifetime == 0 {
		opts.MaxLifetime = DefaultMaxLifetime
	}

	if opts.StreamBackoff == nil {
		opts.StreamBackoff = DefaultStreamBackoff
	}

	return opts
}
