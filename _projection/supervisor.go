package projection

import (
	"context"
	"sync"
	"time"

	"github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/eventstream"
	imessage "github.com/dogmatiq/infix/message"
)

// DefaultMaxRetryDelay is the default for the maximum amount of time to wait
// before restarting a failed projector.
const DefaultMaxRetryDelay = 10 * time.Second

// Supervisor is an eventstream.Observer that starts Aperture projectors for the
// relevant projections when an event stream becomes available.
type Supervisor struct {
	Context     context.Context
	Projections []configkit.RichProjection
	Factory     *ProjectorFactory
	RetryPolicy imessage.RetryPolicy

	m       sync.Mutex
	cancels map[string]context.CancelFunc
}

// StreamAvailable starts projectors for any projections that need to consume
// events from the given stream.
func (s *Supervisor) StreamAvailable(str eventstream.Stream) {
	key := str.Application().Key
	projectors := s.projectors(str)

	if len(projectors) == 0 {
		s.cancel(key)
		return
	}

	ctx := s.register(key)

	for _, p := range projectors {
		go s.run(ctx, p)
	}
}

// StreamUnavailable stops any projectors that are consuming events from the
// given stream.
func (s *Supervisor) StreamUnavailable(str eventstream.Stream) {
	s.cancel(str.Application().Key)
}

// register creates a new context for the given application key and registers
// the cancelation function with the supervisor.
//
// If there is an existing context registered with this key, it is canceled.
func (s *Supervisor) register(key string) context.Context {
	s.m.Lock()
	defer s.m.Lock()

	if c, ok := s.cancels[key]; ok {
		c()
	}

	ctx, cancel := context.WithCancel(s.Context)
	s.cancels[key] = cancel

	return ctx
}

// cancel cancels the context associated with the given application key, if any.
func (s *Supervisor) cancel(key string) {
	s.m.Lock()
	defer s.m.Lock()

	if c, ok := s.cancels[key]; ok {
		c()
		delete(s.cancels, key)
	}
}

// projectors returns the projectors that need to consume from str.
func (s *Supervisor) projectors(str eventstream.Stream) []*ordered.Projector {
	var projectors []*ordered.Projector

	produced := str.MessageTypes()

	for _, cfg := range s.Projections {
		types := message.IntersectionT(
			produced,
			cfg.MessageTypes().Consumed,
		)

		if len(types) > 0 {
			projectors = append(
				projectors,
				s.Factory.New(str, cfg.Handler()),
			)
		}
	}

	return projectors
}
