package projection

import (
	"time"

	"github.com/dogmatiq/aperture/ordered"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/eventstream"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/trace"
)

// ProjectorFactory constructs new Aperture projectors.
type ProjectorFactory struct {
	DefaultTimeout time.Duration
	Logger         logging.Logger
	Meter          metric.Meter
	Tracer         trace.Tracer
}

// New returns a projector used to apply events from s to h.
func (f *ProjectorFactory) New(
	s eventstream.Stream,
	h dogma.ProjectionMessageHandler,
) *ordered.Projector {
	return &ordered.Projector{
		Stream:         &StreamAdaptor{s},
		Handler:        h,
		Logger:         f.Logger,
		DefaultTimeout: f.DefaultTimeout,
		Metrics:        nil, // TODO
		Tracer:         f.Tracer,
	}
}
