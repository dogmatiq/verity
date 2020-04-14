package fixtures

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	"github.com/google/uuid"
)

// NewEnvelopeProto returns a new envelope containing the given message,
// marshaled to its protobuf representation.
//
// If id is empty, a new UUID is generated.
//
// times can contain up to two elements, the first is the created time, the
// second is the scheduled-for time.
func NewEnvelopeProto(
	id string,
	m dogma.Message,
	times ...time.Time,
) *envelopespec.Envelope {
	return NewParcel(id, m, times...).Envelope
}

// NewEnvelope returns a new envelope containing the given message.
//
// If id is empty, a new UUID is generated.
//
// times can contain up to two elements, the first is the created time, the
// second is the scheduled-for time.
func NewEnvelope(
	id string,
	m dogma.Message,
	times ...time.Time,
) *envelope.Envelope {
	if id == "" {
		id = uuid.New().String()
	}

	env := &envelope.Envelope{
		MetaData: envelope.MetaData{
			MessageID:     id,
			CausationID:   "<cause>",
			CorrelationID: "<correlation>",
			Source: envelope.Source{
				Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
				Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
				InstanceID:  "<instance>",
			},
		},
		Message: m,
	}

	switch len(times) {
	case 0:
		env.MetaData.CreatedAt = time.Now()
	case 1:
		env.MetaData.CreatedAt = times[0]
	case 2:
		env.MetaData.CreatedAt = times[0]
		env.MetaData.ScheduledFor = times[1]
	default:
		panic("too many times specified")
	}

	cleanseTime(&env.MetaData.CreatedAt)
	cleanseTime(&env.MetaData.ScheduledFor)

	return env
}
