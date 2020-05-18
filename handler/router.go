package handler

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/parcel"
)

// MessageTypeRouter is a handler that dispatches to other handlers based on
// message type.
type MessageTypeRouter map[message.Type]Handler

// HandleMessage handles the message in p.
func (r MessageTypeRouter) HandleMessage(
	ctx context.Context,
	w *UnitOfWork,
	p *parcel.Parcel,
) error {
	mt := message.TypeOf(p.Message)

	if h, ok := r[mt]; ok {
		return h.HandleMessage(ctx, w, p)
	}

	return fmt.Errorf("no route for '%s' messages", mt)
}
