package handler

import (
	"context"
	"fmt"

	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/verity/parcel"
)

// Router is a handler that dispatches to other handlers based on
// message type.
type Router map[message.Type][]Handler

// HandleMessage handles the message in p.
func (r Router) HandleMessage(
	ctx context.Context,
	w UnitOfWork,
	p parcel.Parcel,
) error {
	mt := message.TypeOf(p.Message)

	handlers := r[mt]

	if len(handlers) == 0 {
		return fmt.Errorf("no route for '%s' messages", mt)
	}

	for _, h := range handlers {
		if err := h.HandleMessage(ctx, w, p); err != nil {
			return err
		}
	}

	return nil
}
