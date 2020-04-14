package memory

import (
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/infix/persistence/subsystem/queuestore"
	"github.com/golang/protobuf/proto"
)

func cloneEventStoreItem(i *eventstore.Item) *eventstore.Item {
	clone := *i
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}

func cloneQueueStoreItem(i *queuestore.Item) *queuestore.Item {
	clone := *i
	clone.Envelope = cloneEnvelope(clone.Envelope)
	return &clone
}

func cloneEnvelope(env *envelopespec.Envelope) *envelopespec.Envelope {
	return proto.Clone(env).(*envelopespec.Envelope)
}
