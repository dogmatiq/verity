package memory

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/golang/protobuf/proto"
)

func cloneEnvelope(env *envelopespec.Envelope) *envelopespec.Envelope {
	return proto.Clone(env).(*envelopespec.Envelope)
}
