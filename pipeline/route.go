package pipeline

import (
	"context"

	"github.com/dogmatiq/configkit/message"
)

// RouteByType returns a stage that routes to different stages based on the type
// of the message.
func RouteByType(table map[message.Type]Stage) Stage {
	return func(ctx context.Context, sc *Scope, next Sink) error {
		m, err := sc.Session.Message()
		if err != nil {
			return err
		}

		mt := message.TypeOf(m)

		if n, ok := table[mt]; ok {
			return n(ctx, sc, next)
		}

		return next(ctx, sc)
	}
}
