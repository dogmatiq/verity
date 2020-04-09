package pipeline

import (
	"context"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/marshalkit"
	"golang.org/x/sync/errgroup"
)

// Pump obtains sessions from a source and feeds them to a sink.
func Pump(
	ctx context.Context,
	m marshalkit.Marshaler,
	l logging.Logger,
	from Source, to Sink,
) error {
	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for {
			sess, err := from(ctx)
			if err != nil {
				return err
			}

			g.Go(func() error {
				defer sess.Close()

				sc := &Scope{
					Session:   sess,
					Marshaler: m,
					Logger:    l,
				}

				return to(ctx, sc)
			})
		}
	})

	<-ctx.Done()
	return g.Wait()
}
