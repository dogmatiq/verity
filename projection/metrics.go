package projection

import "go.opentelemetry.io/otel/api/metric"

// Metrics encapsulates the metrics collected by a Projector.
type Metrics struct {
	// HandlingTime is the bound metric used to record the amount of time spent
	// handling each message, in seconds.
	HandlingTime metric.BoundFloat64Measure

	// Offset is the bound handle used to record last offset that was
	// successfully applied to the projection.
	Offset metric.BoundInt64Gauge

	// Errors is the bound metric used to record the number of errors that occur
	// while attempting to handle messages.
	Errors metric.BoundInt64Counter

	// Conflicts is the bound metric used to record the number of OCC conflicts
	// that occur while attempting to handle messages.
	Conflicts metric.BoundInt64Counter
}
