package pipeline

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// ScopeFactory creates a scope for a session.
type ScopeFactory func(Session) *Scope

// Pump obtains sessions from a source and feeds them to a sink.
func Pump(
	ctx context.Context,
	new ScopeFactory,
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
				sc := new(sess)
				return to(ctx, sc)
			})
		}
	})

	<-ctx.Done()
	return g.Wait()
}
