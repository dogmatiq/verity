package memory

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/golang/protobuf/proto"
)

func cloneEventStoreParcel(p *eventstore.Parcel) *eventstore.Parcel {
	clone := *p
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}

func cloneQueueStoreParcel(p *queuestore.Parcel) *queuestore.Parcel {
	clone := *p
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}

func cloneEnvelope(env *envelopespec.Envelope) *envelopespec.Envelope {
	return proto.Clone(env).(*envelopespec.Envelope)
}
