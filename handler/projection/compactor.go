package projection

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/linger"
)

// Compactor periodically compacts a projection.
type Compactor struct {
	// Handler is the projection message handler to be compacted.
	Handler dogma.ProjectionMessageHandler

	// Interval is the interval at which the projection is compacted.
	Interval time.Duration

	// Timeout is the default timeout to use when compacting the projection.
	Timeout time.Duration

	// Logger is the target for log messages produced about compaction.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// Run periodically compacts the projection until ctx is canceled or an error
// occurs.
func (c *Compactor) Run(ctx context.Context) error {
	for {
		if err := c.compact(ctx); err != nil {
			return err
		}

		if err := linger.Sleep(ctx, c.Interval); err != nil {
			return err
		}
	}
}

// compact performs compaction. It returns an error if compaction fails for any
// reason other than a timeout.
func (c *Compactor) compact(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()

	if err := c.Handler.Compact(
		ctx,
		compactScope{
			logger: c.Logger,
		},
	); err != nil {
		if err != context.DeadlineExceeded {
			// The error was something other than a timeout of the compaction
			// process itself.
			return err
		}

		// Otherwise, the compaction timed out, but this is allowed. Log about
		// it but continue as normal.
		logging.Log(c.Logger, "compaction timed out, retrying later")
	}

	logging.Log(c.Logger, "compaction completed")

	return nil
}
