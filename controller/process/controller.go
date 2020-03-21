package process

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger"
)

// DefaultTimeout is the default timeout to use when applying an event.
const DefaultTimeout = 3 * time.Second

// Controller orchestrates the handling of a message by a process message
// handler.
type Controller struct {
	// HandlerConfig is the confugration of the process message handler.
	HandlerConfig configkit.RichProcess

	// Packer is used to construct new message envelopes.
	Packer *envelope.Packer

	// DefaultTimeout is the maximum time to allow for handling a single event
	// if the handler does not provide a timeout hint. If it is nil,
	// DefaultTimeout is used.
	DefaultTimeout time.Duration

	// Logger is the target for log messages produced within the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger
}

// Handle handles a message.
func (c *Controller) Handle(
	ctx context.Context,
	tx persistence.Transaction,
	env *envelope.Envelope,
) error {
	mt := message.TypeOf(env.Message)
	role, ok := c.HandlerConfig.MessageTypes().Consumed[mt]
	if !ok {
		panic(dogma.UnexpectedMessage)
	}

	hk := c.HandlerConfig.Identity().Key
	h := c.HandlerConfig.Handler()

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		h.TimeoutHint(env.Message),
		c.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	id, ok, err := c.route(ctx, role, env)
	if !ok || err != nil {
		return err
	}

	root, rev, err := tx.LoadProcess(ctx, hk, id)
	if err != nil {
		return err
	}

	if rev == 0 {
		root = h.New()
	}

	s := &scope{
		packer: c.Packer.Bind(env, c.HandlerConfig, id),
		logger: c.Logger,

		root:   root,
		rev:    rev,
		exists: rev != 0,
	}

	if err := c.handle(ctx, role, s, env); err != nil {
		return err
	}

	return c.persist(ctx, tx, id, s)
}

// route returns the instance that the message should be routed to.
func (c *Controller) route(
	ctx context.Context,
	role message.Role,
	env *envelope.Envelope,
) (string, bool, error) {
	if role == message.TimeoutRole {
		return env.Source.InstanceID, true, nil
	}

	return c.HandlerConfig.Handler().RouteEventToInstance(ctx, env.Message)
}

// handle passes the message to the appropriate handler method.
func (c *Controller) handle(
	ctx context.Context,
	role message.Role,
	s *scope,
	env *envelope.Envelope,
) error {
	if role == message.TimeoutRole {
		return c.HandlerConfig.Handler().HandleTimeout(ctx, s, env.Message)
	}

	return c.HandlerConfig.Handler().HandleEvent(ctx, s, env.Message)
}

// persist saves the changes captured by the scope.
func (c *Controller) persist(
	ctx context.Context,
	tx persistence.Transaction,
	id string,
	s *scope,
) error {
	hk := c.HandlerConfig.Identity().Key

	for _, env := range s.commands {
		if err := tx.Enqueue(ctx, env); err != nil {
			return err
		}
	}

	for _, env := range s.timeouts {
		if err := tx.Enqueue(ctx, env); err != nil {
			return err
		}
	}

	if s.exists {
		return tx.SaveProcess(
			ctx,
			hk,
			id,
			s.rev,
			s.root,
		)
	}

	if s.rev > 0 {
		return tx.DeleteProcess(
			ctx,
			hk,
			id,
			s.rev,
		)
	}

	return nil
}
