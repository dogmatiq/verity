package infix

import (
	"context"
	"fmt"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/envelopespec"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/eventstream/memorystream"
	"github.com/dogmatiq/infix/eventstream/persistedstream"
	"github.com/dogmatiq/infix/handler"
	"github.com/dogmatiq/infix/handler/aggregate"
	"github.com/dogmatiq/infix/handler/cache"
	"github.com/dogmatiq/infix/handler/integration"
	"github.com/dogmatiq/infix/handler/process"
	"github.com/dogmatiq/infix/internal/x/loggingx"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/queue"
)

type app struct {
	Config      configkit.RichApplication
	DataStore   persistence.DataStore
	EventCache  *memorystream.Stream
	EventStream *persistedstream.Stream
	Queue       *queue.Queue
	EntryPoint  *handler.EntryPoint
	Logger      logging.Logger
}

// initApp initializes the engine to handle the app represented by cfg.
func (e *Engine) initApp(
	ctx context.Context,
	cfg configkit.RichApplication,
) error {
	logging.Log(
		e.logger,
		"hosting @%s application, identity key is %s",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	id := cfg.Identity()

	ds, err := e.dataStores.Get(ctx, id.Key)
	if err != nil {
		return fmt.Errorf(
			"unable to open data-store for %s: %w",
			cfg.Identity(),
			err,
		)
	}

	c, err := e.newEventCache(ctx, cfg, ds)
	if err != nil {
		return err
	}

	q := e.newQueue(cfg, ds)
	s := e.newEventStream(cfg, ds, c)
	x := e.newCommandExecutor(cfg, ds, q)
	l := loggingx.WithPrefix(
		e.opts.Logger,
		"@%s  ",
		cfg.Identity().Name,
	)
	ep := e.newEntryPoint(cfg, ds, q, c, l)

	a := &app{
		Config:      cfg,
		DataStore:   ds,
		EventCache:  c,
		EventStream: s,
		Queue:       q,
		EntryPoint:  ep,
		Logger:      l,
	}

	if e.apps == nil {
		e.apps = map[string]*app{}
		e.executors = map[message.Type]dogma.CommandExecutor{}
	}

	e.apps[id.Key] = a

	for mt := range x.Packer.Produced {
		e.executors[mt] = x
	}

	return nil
}

// newQueue returns a new queue for a specific app.
func (e *Engine) newQueue(
	cfg configkit.RichApplication,
	ds persistence.DataStore,
) *queue.Queue {
	return &queue.Queue{
		Repository: ds,
		Marshaler:  e.opts.Marshaler,
		// TODO: https://github.com/dogmatiq/infix/issues/102
		// Make buffer size configurable.
		BufferSize: 0,
	}
}

// newEventCache returns a new memory stream used as the event cache.
func (e *Engine) newEventCache(
	ctx context.Context,
	cfg configkit.RichApplication,
	ds persistence.DataStore,
) (*memorystream.Stream, error) {
	next, err := ds.NextEventOffset(ctx)
	if err != nil {
		return nil, err
	}

	return &memorystream.Stream{
		App:         cfg.Identity(),
		FirstOffset: next,
		// TODO: https://github.com/dogmatiq/infix/issues/226
		// Make buffer size configurable.
		BufferSize: 0,
		Types: cfg.
			MessageTypes().
			Produced.
			FilterByRole(message.EventRole),
	}, nil
}

// newEventStream returns a new event stream for a specific app.
func (e *Engine) newEventStream(
	cfg configkit.RichApplication,
	ds persistence.DataStore,
	cache eventstream.Stream,
) *persistedstream.Stream {
	return &persistedstream.Stream{
		App:        cfg.Identity(),
		Repository: ds,
		Marshaler:  e.opts.Marshaler,
		Cache:      cache,
		// TODO: https://github.com/dogmatiq/infix/issues/76
		// Make pre-fetch buffer size configurable.
		PreFetch: 10,
		Types: cfg.
			MessageTypes().
			Produced.
			FilterByRole(message.EventRole),
	}
}

// newCommandExecutor returns a dogma.CommandExecutor for a specific app.
func (e *Engine) newCommandExecutor(
	cfg configkit.RichApplication,
	p persistence.Persister,
	q *queue.Queue,
) *queue.CommandExecutor {
	return &queue.CommandExecutor{
		Queue:     q,
		Persister: p,
		Packer: &parcel.Packer{
			Application: envelopespec.MarshalIdentity(cfg.Identity()),
			Marshaler:   e.opts.Marshaler,
			Produced: cfg.
				MessageTypes().
				Consumed.
				FilterByRole(message.CommandRole),
		},
	}
}

// newEntryPoint returns a new handler entry point for a specific app.
func (e *Engine) newEntryPoint(
	cfg configkit.RichApplication,
	ds persistence.DataStore,
	q *queue.Queue,
	c *memorystream.Stream,
	l logging.Logger,
) *handler.EntryPoint {
	hf := &handlerFactory{
		opts:        e.opts,
		queue:       q,
		queueEvents: message.TypeSet{},
		aggregateLoader: &aggregate.Loader{
			AggregateRepository: ds,
			EventRepository:     ds,
			Marshaler:           e.opts.Marshaler,
		},
		processLoader: &process.Loader{
			Repository: ds,
			Marshaler:  e.opts.Marshaler,
		},
		appLogger:    l,
		engineLogger: e.logger,
	}

	if err := cfg.AcceptRichVisitor(context.Background(), hf); err != nil {
		panic(err)
	}

	return &handler.EntryPoint{
		QueueEvents: hf.queueEvents,
		Handler:     hf.handler,
		OnSuccess: func(r handler.Result) {
			c.Add(r.Events)
			q.Add(r.Queued)
		},
	}
}

// handlerFactory is a configkit.RichVisitor that constructs the handler used by
// the entry point.
type handlerFactory struct {
	opts            *engineOptions
	queue           *queue.Queue
	queueEvents     message.TypeSet
	aggregateLoader *aggregate.Loader
	processLoader   *process.Loader
	engineLogger    logging.Logger
	appLogger       logging.Logger

	app     *envelopespec.Identity
	handler handler.Router
}

func (f *handlerFactory) VisitRichApplication(ctx context.Context, cfg configkit.RichApplication) error {
	f.app = envelopespec.MarshalIdentity(cfg.Identity())
	f.handler = handler.Router{}
	return cfg.RichHandlers().AcceptRichVisitor(ctx, f)
}

func (f *handlerFactory) VisitRichAggregate(_ context.Context, cfg configkit.RichAggregate) error {
	f.addRoutes(cfg, &aggregate.Adaptor{
		Identity: envelopespec.MarshalIdentity(cfg.Identity()),
		Handler:  cfg.Handler(),
		Loader:   f.aggregateLoader,
		Cache: cache.Cache{
			// TODO: https://github.com/dogmatiq/infix/issues/193
			// Make TTL configurable.
			Logger: loggingx.WithPrefix(
				f.engineLogger,
				"[cache %s@%s] ",
				cfg.Identity().Name,
				f.app.Name,
			),
		},
		Packer: &parcel.Packer{
			Application: f.app,
			Marshaler:   f.opts.Marshaler,
			Produced:    cfg.MessageTypes().Produced,
			Consumed:    cfg.MessageTypes().Consumed,
		},
		LoadTimeout: f.opts.MessageTimeout,
		Logger:      f.appLogger,
	})

	return nil
}

func (f *handlerFactory) VisitRichProcess(_ context.Context, cfg configkit.RichProcess) error {
	f.addRoutes(cfg, &process.Adaptor{
		Identity:  envelopespec.MarshalIdentity(cfg.Identity()),
		Handler:   cfg.Handler(),
		Loader:    f.processLoader,
		Marshaler: f.opts.Marshaler,
		Cache: cache.Cache{
			// TODO: https://github.com/dogmatiq/infix/issues/193
			// Make TTL configurable.
			Logger: loggingx.WithPrefix(
				f.engineLogger,
				"[cache %s@%s] ",
				cfg.Identity().Name,
				f.app.Name,
			),
		},
		Queue: f.queue,
		Packer: &parcel.Packer{
			Application: f.app,
			Marshaler:   f.opts.Marshaler,
			Produced:    cfg.MessageTypes().Produced,
			Consumed:    cfg.MessageTypes().Consumed,
		},
		LoadTimeout: f.opts.MessageTimeout,
		Logger:      f.appLogger,
	})

	return nil
}

func (f *handlerFactory) VisitRichIntegration(_ context.Context, cfg configkit.RichIntegration) error {
	f.addRoutes(cfg, &integration.Adaptor{
		Identity:       envelopespec.MarshalIdentity(cfg.Identity()),
		Handler:        cfg.Handler(),
		DefaultTimeout: f.opts.MessageTimeout,
		Packer: &parcel.Packer{
			Application: f.app,
			Marshaler:   f.opts.Marshaler,
			Produced:    cfg.MessageTypes().Produced,
			Consumed:    cfg.MessageTypes().Consumed,
		},
		Logger: f.appLogger,
	})

	return nil
}

func (f *handlerFactory) VisitRichProjection(_ context.Context, cfg configkit.RichProjection) error {
	return nil
}

func (f *handlerFactory) addRoutes(cfg configkit.RichHandler, h handler.Handler) {
	for mt, r := range cfg.MessageTypes().Consumed {
		f.handler[mt] = append(f.handler[mt], h)

		if r == message.EventRole {
			f.queueEvents.Add(mt)
		}

		logging.Debug(
			f.engineLogger,
			"[queue@%s -> %s@%s] consuming '%s' %ss",
			f.app.Name,
			cfg.Identity().Name,
			f.app.Name,
			mt,
			r,
		)
	}
}
