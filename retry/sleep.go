package retry

import (
	"context"
	"time"

	"github.com/dogmatiq/linger"
)

// Sleep blocks until a message is due to be retried, or until ctx is canceled.
func Sleep(
	ctx context.Context,
	p Policy,
	retries int,
	cause ...error,
) error {
	return linger.SleepUntil(ctx, p.NextRetry(
		time.Now(),
		retries,
		cause,
	))
}
