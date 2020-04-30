package networkstream

import (
	"github.com/dogmatiq/marshalkit"
)

// NoopUnmarshaler is a marshalkit.Marshaler decorator that always always
// returns a nil result from Unmarshal().
//
// It is a simple optimization for the networkstream server that prevents
// unnecessary unmarshaling of messages that are served in their marshaled-form.
type NoopUnmarshaler struct {
	marshalkit.Marshaler
}

func (NoopUnmarshaler) Unmarshal(marshalkit.Packet) (interface{}, error) {
	return nil, nil
}
