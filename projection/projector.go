package projection

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/retry"
	"github.com/dogmatiq/linger"
	"go.opentelemetry.io/otel/api/trace"
)

// Projector reads events from a stream and applies them to a projection.
type Projector struct {
	// Stream is the stream used to obtain event messages.
	Stream eventstream.Stream

	// HandlerConfig is the configuration of the projection message handler.
	HandlerConfig configkit.RichProjection

	// DefaultTimeout is the timeout duration to use when hanlding an event if
	// the handler does not provide a timeout hint. It must be set.
	DefaultTimeout time.Duration

	// RetryPolicy is the policy used to determine how long to wait before
	// restarting the projector after an error occurs.
	RetryPolicy retry.Policy

	// Logger is the target for log messages from the projector and the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	// Metrics contains the metrics recorded by the projector. If it is nil no
	// metrics are recorded.
	Metrics *Metrics

	// Tracer is used to record tracing spans. If it is nil, no tracing is
	// performed.
	Tracer trace.Tracer

	// retries is the number of times the projector has failed to start applying
	// events since the last successful attempt.
	retries int

	// resource is the name of the projection OCC resource. It is a binary
	// representation of the stream's source application key.
	resource []byte

	// current is the expected current version of the OCC resource. It is
	// initially read from the projection.
	current []byte

	// next is the next version of the OCC resource. It is a binary
	// representation of the event's offset.
	next []byte
}

// Run applies events to a projection until ctx is canceled.
//
// If an error occurs either reading from the event stream, or applying the
// event to the projection, the projector is restarted after a delay as per
// p.RetryPolicy.
func (p *Projector) Run(ctx context.Context) error {
	p.resource = []byte(p.Stream.Application().Key)

	for {
		err := p.consume(ctx)

		// bail immediately if the ctx is canceled
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// TODO: log error
		p.Metrics.Errors.Add(ctx, 1)

		if err := retry.Sleep(ctx, p.RetryPolicy, p.retries, err); err != nil {
			return err
		}
	}
}

// consume reads messages from the stream and applies them to the projection
// until ctx is canceled or an error occurs.
func (p *Projector) consume(ctx context.Context) error {
	offset, err := p.loadOffset(ctx)
	if err != nil {
		return err
	}

	cur, err := p.Stream.Open(ctx, offset, p.HandlerConfig.MessageTypes().Consumed)
	if err != nil {
		return err
	}
	defer cur.Close()

	// TODO: log started consuming, offset, handler, etc

	for {
		if err := p.consumeNext(ctx, cur); err != nil {
			return err
		}
	}
}

// consumeNext reads the next message from cur and applies it to the projection.
func (p *Projector) consumeNext(ctx context.Context, cur eventstream.Cursor) error {
	env, err := cur.Next(ctx)
	if err != nil {
		return err
	}

	binary.BigEndian.PutUint64(p.next, env.StreamOffset)

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		p.HandlerConfig.Handler().TimeoutHint(env.Message),
		p.DefaultTimeout,
	)
	defer cancel()

	ok, err := p.HandlerConfig.Handler().HandleEvent(
		ctx,
		p.resource,
		p.current,
		p.next,
		scope{
			recordedAt: env.CreatedAt,
			logger:     p.Logger,
		},
		env.Message,
	)

	if err != nil {
		return err
	}

	if !ok {
		p.Metrics.Conflicts.Add(ctx, 1)
		return errors.New("optimistic concurrency conflict")
	}

	p.Metrics.Offset.Set(ctx, int64(env.StreamOffset))

	// next is now current, re-use the old buffer for the next resource version
	p.current, p.next = p.next, p.current

	return nil
}

// loadOffset loads the previously applied offset from the projection and
// populates p.current and p.next.
//
// It returns the numeric representation of the next offset to be applied.
func (p *Projector) loadOffset(ctx context.Context) (uint64, error) {
	// TODO: ctx, span := tracing.StartSpan(ctx, p.Tracer)

	p.next = make([]byte, 8)

	var err error
	p.current, err = p.HandlerConfig.Handler().ResourceVersion(ctx, p.resource)
	if err != nil {
		return 0, err
	}

	switch len(p.current) {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(p.current) + 1, nil
	default:
		return 0, fmt.Errorf(
			"the persisted version is %d byte(s), expected 0 or 8",
			len(p.current),
		)
	}
}
