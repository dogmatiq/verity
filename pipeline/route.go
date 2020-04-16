package pipeline

import (
	"context"

	"github.com/dogmatiq/configkit/message"
)

// RouteByType returns a stage that routes to different stages based on the type
// of the message.
func RouteByType(table map[message.Type]Stage) Stage {
	return func(ctx context.Context, req Request, res *Response, next Sink) error {
		p, err := req.Parcel()
		if err != nil {
			return err
		}

		mt := message.TypeOf(p.Message)

		if n, ok := table[mt]; ok {
			return n(ctx, req, res, next)
		}

		return next(ctx, req, res)
	}
}
