package projection

import (
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/eventstream"
	imessage "github.com/dogmatiq/infix/message"
	"github.com/dogmatiq/linger"
	"go.opentelemetry.io/otel/api/core"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/trace"
)

// ProjectorMetrics encapsulates the metrics collected by a Projector.
type ProjectorMetrics struct {
	// HandleTimeMeasure is the bound metric used to record the amount of time
	// spent handling each message, in seconds.
	HandleTimeMeasure metric.BoundFloat64Measure

	// ConflictCount is the bound metric used to record the number of OCC
	// conflicts that occur while attempting to handle messages.
	ConflictCount metric.BoundInt64Counter

	// OffsetGauge is the bound handle used to record last offset that was
	// successfully applied to the projection.
	OffsetGauge metric.BoundInt64Gauge
}

// Projector reads events from a stream and applies them to a projection.
type Projector struct {
	// Stream is the stream used to obtain event messages.
	Stream eventstream.Stream

	// Config is the configuration of the projection message handler.
	Config configkit.RichProjection

	// DefaultTimeout is the timeout duration to use when hanlding an event if
	// the handler does not provide a timeout hint. If it is zero the global
	// DefaultTimeout constant is used.
	DefaultTimeout time.Duration

	// RetryPolicy is the policy used to determine how long to wait before
	// restarting the projector after an error occurs.
	RetryPolicy imessage.RetryPolicy

	// Logger is the target for log messages from the projector and the handler.
	// If it is nil, logging.DefaultLogger is used.
	Logger logging.Logger

	// Metrics contains the metrics recorded by the projector. If it is nil no
	// metrics are recorded.
	Metrics *ProjectorMetrics

	// Tracer is used to record tracing spans. If it is nil, no tracing is
	// performed.
	Tracer trace.Tracer

	name       string
	types      message.TypeCollection
	resource   []byte
	current    []byte
	next       []byte
	nameAttr   core.KeyValue
	keyAttr    core.KeyValue
	streamAttr core.KeyValue
}

// Run applies events to a projection until ctx is canceled or an error occurs.
//
// If message handling fails due to an optimistic concurrency conflict within
// the projection the consumer restarts automatically. Any other error is
// returned, in which case it is the caller's responsibility to implement any
// retry logic. Run() can safely be called again after exiting with an error.
func (p *Projector) Run(ctx context.Context) error {
	p.name = p.Config.Identity().Name
	p.types = p.Config.MessageTypes().Consumed
	p.resource = []byte(p.Stream.ID())

	p.nameAttr = tracing.HandlerName.String(cfg.Identity().Name)
	p.keyAttr = tracing.HandlerKey.String(cfg.Identity().Key)
	p.streamAttr = tracing.StreamID.String(p.Stream.ID())

	for {
		if err := p.consume(ctx); err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf(
					"unable to consume from '%s' for the '%s' projection: %w",
					p.Stream.ID(),
					p.name,
					err,
				)
			}
		}
	}
}

// consume opens the streams, consumes messages ands applies them to the
// projection.
//
// It consumes until ctx is canceled, and error occurs, or a message is not
// applied due to an OCC conflict, in which case it returns nil.
func (p *Projector) consume(ctx context.Context) error {
	cur, err := p.open(ctx)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		ok, err := p.consumeNext(ctx, cur)
		if !ok || err != nil {
			return err
		}
	}
}

// open opens a cursor on the stream based on the offset recorded within the
// projection.
func (p *Projector) open(ctx context.Context) (Cursor, error) {
	var types []dogma.Message
	p.types.Range(func(t message.Type) bool {
		types = append(types, reflect.Zero(t.ReflectType()).Interface())
		return true
	})

	var offset uint64

	if err := tracing.WithSpan(
		ctx,
		p.Tracer,
		"query last offset",
		func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(
				p.nameAttr,
				p.keyAttr,
				tracing.HandlerTypeProjectionAttr,
				p.streamAttr,
			)

			var err error
			p.current, err = p.Handler.ResourceVersion(ctx, p.resource)
			if err != nil {
				return err
			}

			switch len(p.current) {
			case 0:
				offset = 0
			case 8:
				offset = binary.BigEndian.Uint64(p.current) + 1
			default:
				return fmt.Errorf(
					"the persisted version is %d byte(s), expected 0 or 8",
					len(p.current),
				)
			}

			span.SetAttributes(
				tracing.StreamOffset.Uint64(offset),
			)

			return nil
		},
	); err != nil {
		return nil, err
	}

	logging.Log(
		p.Logger,
		"[%s %s@%d] started consuming",
		p.name,
		p.resource,
		offset,
	)

	return p.Stream.Open(ctx, offset, types)
}

// consumeNext waits for the next message on the stream then applies it to the
// projection.
func (p *Projector) consumeNext(ctx context.Context, cur Cursor) (bool, error) {
	env, err := cur.Next(ctx)
	if err != nil {
		return false, err
	}

	if p.next == nil {
		p.next = make([]byte, 8)
	}

	binary.BigEndian.PutUint64(p.next, env.Offset)

	ctx, cancel := linger.ContextWithTimeout(
		ctx,
		p.Handler.TimeoutHint(env.Message),
		p.DefaultTimeout,
		DefaultTimeout,
	)
	defer cancel()

	mt := message.TypeOf(env.Message).String()

	var ok bool
	if err := tracing.WithSpan(
		ctx,
		p.Tracer,
		mt,
		func(ctx context.Context) error {
			trace.SpanFromContext(ctx).SetAttributes(
				p.nameAttr,
				p.keyAttr,
				tracing.HandlerTypeProjectionAttr,
				p.streamAttr,
				tracing.StreamOffset.Uint64(env.Offset),
				tracing.MessageType.String(mt),
				tracing.MessageRoleEventAttr,
				tracing.MessageDescription.String(dogma.DescribeMessage(env.Message)),
				tracing.MessageRecordedAt.String(env.RecordedAt.Format(time.RFC3339Nano)),
			)

			var err error
			start := time.Now()
			ok, err = p.Handler.HandleEvent(
				ctx,
				p.resource,
				p.current,
				p.next,
				scope{
					resource:   p.resource,
					offset:     env.Offset,
					handler:    p.name,
					recordedAt: env.RecordedAt,
					logger:     p.Logger,
				},
				env.Message,
			)
			if p.Metrics != nil {
				p.Metrics.HandleTimeMeasure.Record(ctx, time.Since(start).Seconds())
			}

			return err
		},
	); err != nil {
		return false, err
	}

	if ok {
		// keep swapping between the two buffers to avoid repeat allocations
		p.current, p.next = p.next, p.current

		if p.Metrics != nil {
			p.Metrics.OffsetGauge.Set(ctx, int64(env.Offset))
		}

		return true, nil
	}

	logging.Log(
		p.Logger,
		"[%s %s@%d] an optimisitic concurrency conflict occurred, restarting the consumer",
		p.name,
		p.resource,
		env.Offset,
	)

	if p.Metrics != nil {
		p.Metrics.ConflictCount.Add(ctx, 1)
	}

	return false, nil
}
