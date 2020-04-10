package bboltx

import (
	"context"
	"os"

	"github.com/dogmatiq/linger"
	"go.etcd.io/bbolt"
)

// Open creates and opens a database at the given path.
//
// If mode is zero, 0600 is used.
//
// If the deadline from ctx is sooner than opts.Timeout, the context deadline is
// used instead.
func Open(
	ctx context.Context,
	path string,
	mode os.FileMode,
	opts *bbolt.Options,
) (*bbolt.DB, error) {
	if mode == 0 {
		mode = 0600
	}

	if ctx.Err() != nil {
		// Bail early if the context is already ended. This is necessary because
		// if we put a non-positive timeout in the BoltDB options it will just
		// use the default timeout.
		return nil, ctx.Err()
	}

	if timeout, ok := linger.FromContextDeadline(ctx); ok {
		if opts == nil {
			clone := *bbolt.DefaultOptions
			opts = &clone
			opts.Timeout = timeout
		} else if opts.Timeout == 0 || opts.Timeout > timeout {
			clone := *opts
			opts = &clone
			opts.Timeout = timeout
		}
	}

	db, err := bbolt.Open(path, mode, opts)

	if err != nil && err.Error() == "timeout" {
		err = context.DeadlineExceeded
	}

	return db, err
}
