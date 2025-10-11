package verity

import (
	"context"
	"fmt"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/eventstream/memorystream"
	"github.com/dogmatiq/verity/eventstream/persistedstream"
	"github.com/dogmatiq/verity/handler"
	"github.com/dogmatiq/verity/handler/aggregate"
	"github.com/dogmatiq/verity/handler/cache"
	"github.com/dogmatiq/verity/handler/integration"
	"github.com/dogmatiq/verity/handler/process"
	"github.com/dogmatiq/verity/internal/x/loggingx"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	"github.com/dogmatiq/verity/queue"
)

type app struct {
	Config      *config.Application
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
	cfg *config.Application,
) error {
	logging.Log(
		e.logger,
		"hosting @%s application, identity key is %s",
		cfg.Identity().Name,
		cfg.Identity().Key,
	)

	id := cfg.Identity()

	ds, err := e.dataStores.Get(ctx, id.Key.AsString())
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

	q := e.newQueue(ds)
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

	e.apps[id.Key.AsString()] = a

	for mt := range x.Packer.Produced.All() {
		e.executors[mt] = x
	}

	return nil
}

// newQueue returns a new queue for a specific app.
func (e *Engine) newQueue(
	ds persistence.DataStore,
) *queue.Queue {
	return &queue.Queue{
		Repository: ds,
		// TODO: https://github.com/dogmatiq/verity/issues/102
		// Make buffer size configurable.
		BufferSize: 0,
	}
}

// newEventCache returns a new memory stream used as the event cache.
func (e *Engine) newEventCache(
	ctx context.Context,
	cfg *config.Application,
	ds persistence.DataStore,
) (*memorystream.Stream, error) {
	next, err := ds.NextEventOffset(ctx)
	if err != nil {
		return nil, err
	}

	return &memorystream.Stream{
		App:         cfg.Identity(),
		FirstOffset: next,
		// TODO: https://github.com/dogmatiq/verity/issues/226
		// Make buffer size configurable.
		BufferSize: 0,
		Types: cfg.
			RouteSet().
			Filter(config.FilterByMessageKind(message.EventKind)).
			MessageTypeSet(),
	}, nil
}

// newEventStream returns a new event stream for a specific app.
func (e *Engine) newEventStream(
	cfg *config.Application,
	ds persistence.DataStore,
	cache eventstream.Stream,
) *persistedstream.Stream {
	return &persistedstream.Stream{
		App:        cfg.Identity(),
		Repository: ds,
		Cache:      cache,
		// TODO: https://github.com/dogmatiq/verity/issues/76
		// Make pre-fetch buffer size configurable.
		PreFetch: 10,
		Types: cfg.
			RouteSet().
			Filter(config.FilterByMessageKind(message.EventKind)).
			MessageTypeSet(),
	}
}

// newCommandExecutor returns a dogma.CommandExecutor for a specific app.
func (e *Engine) newCommandExecutor(
	cfg *config.Application,
	p persistence.Persister,
	q *queue.Queue,
) *queue.CommandExecutor {
	return &queue.CommandExecutor{
		Queue:     q,
		Persister: p,
		Packer: &parcel.Packer{
			Application: &identitypb.Identity{
				Name: cfg.Identity().Name,
				Key:  cfg.Identity().Key,
			},
			Produced: cfg.
				RouteSet().
				Filter(
					config.FilterByMessageKind(message.CommandKind),
					config.FilterByMessageDirection(config.InboundDirection),
				).
				MessageTypeSet(),
		},
	}
}

// newEntryPoint returns a new handler entry point for a specific app.
func (e *Engine) newEntryPoint(
	cfg *config.Application,
	ds persistence.DataStore,
	q *queue.Queue,
	c *memorystream.Stream,
	l logging.Logger,
) *handler.EntryPoint {
	hf := &handlerFactory{
		opts:        e.opts,
		queue:       q,
		queueEvents: sets.New[message.Type](),
		aggregateLoader: &aggregate.Loader{
			AggregateRepository: ds,
			EventRepository:     ds,
		},
		processLoader: &process.Loader{
			Repository: ds,
		},
		appLogger:    l,
		engineLogger: e.logger,
	}

	hf.visitApplication(cfg)

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
	queueEvents     *sets.Set[message.Type]
	aggregateLoader *aggregate.Loader
	processLoader   *process.Loader
	engineLogger    logging.Logger
	appLogger       logging.Logger

	app     *identitypb.Identity
	handler handler.Router
}

func (f *handlerFactory) visitApplication(cfg *config.Application) {
	f.app = cfg.Identity()
	f.handler = handler.Router{}

	for _, h := range cfg.Handlers() {
		if h.IsDisabled() {
			logging.Log(
				f.engineLogger,
				"[%s@%s] handler is disabled",
				cfg.Identity().Name,
				f.app.Name,
			)
			continue
		}

		switch h := h.(type) {
		case *config.Aggregate:
			f.visitAggregate(h)
		case *config.Process:
			f.visitProcess(h)
		case *config.Integration:
			f.visitIntegration(h)
		}
	}
}

func (f *handlerFactory) visitAggregate(cfg *config.Aggregate) {
	f.addRoutes(cfg, &aggregate.Adaptor{
		Identity: cfg.Identity(),
		Handler:  cfg.Source.Get(),
		Loader:   f.aggregateLoader,
		Cache: cache.Cache{
			// TODO: https://github.com/dogmatiq/verity/issues/193
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
			Produced: cfg.
				RouteSet().
				Filter(config.FilterByMessageDirection(config.OutboundDirection)).
				MessageTypeSet(),
			Consumed: cfg.
				RouteSet().
				Filter(config.FilterByMessageDirection(config.InboundDirection)).
				MessageTypeSet(),
		},
		LoadTimeout: f.opts.MessageTimeout,
		Logger:      f.appLogger,
	})
}

func (f *handlerFactory) visitProcess(cfg *config.Process) {
	f.addRoutes(cfg, &process.Adaptor{
		Identity: cfg.Identity(),
		Handler:  cfg.Source.Get(),
		Loader:   f.processLoader,
		Cache: cache.Cache{
			// TODO: https://github.com/dogmatiq/verity/issues/193
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
			Produced: cfg.
				RouteSet().
				Filter(config.FilterByMessageDirection(config.OutboundDirection)).
				MessageTypeSet(),
			Consumed: cfg.
				RouteSet().
				Filter(config.FilterByMessageDirection(config.InboundDirection)).
				MessageTypeSet(),
		},
		LoadTimeout: f.opts.MessageTimeout,
		Logger:      f.appLogger,
	})
}

func (f *handlerFactory) visitIntegration(cfg *config.Integration) {
	f.addRoutes(cfg, &integration.Adaptor{
		Identity: cfg.Identity(),
		Handler:  cfg.Source.Get(),
		Timeout:  f.opts.MessageTimeout,
		Packer: &parcel.Packer{
			Application: f.app,
			Produced: cfg.
				RouteSet().
				Filter(config.FilterByMessageDirection(config.OutboundDirection)).
				MessageTypeSet(),
			Consumed: cfg.
				RouteSet().
				Filter(config.FilterByMessageDirection(config.InboundDirection)).
				MessageTypeSet(),
		},
		Logger: f.appLogger,
	})
}

func (f *handlerFactory) addRoutes(cfg config.Handler, h handler.Handler) {
	messageTypes := cfg.
		RouteSet().
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		MessageTypes()

	for mt := range messageTypes {
		f.handler[mt] = append(f.handler[mt], h)

		if mt.Kind() == message.EventKind {
			f.queueEvents.Add(mt)
		}

		logging.Debug(
			f.engineLogger,
			"[queue@%s -> %s@%s] consuming '%s' %ss",
			f.app.Name,
			cfg.Identity().Name,
			f.app.Name,
			mt,
			mt.Kind(),
		)
	}
}
