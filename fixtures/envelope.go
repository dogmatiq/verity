package fixtures

import (
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/fixtures"
)

// NewEnvelope returns a new envelope containing the given message.
//
// times can contain up to two elements, the first is the created time, the
// second is the scheduled-for time.
func NewEnvelope(
	id string,
	m dogma.Message,
	times ...time.Time,
) *envelope.Envelope {
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
		Packet: marshalkit.MustMarshalMessage(
			fixtures.Marshaler,
			m,
		),
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

// NewEnvelopeProto returns a new envelope containing the given message,
// marshaled to its protobuf representation.
//
// times can contain up to two elements, the first is the created time, the
// second is the scheduled-for time.
func NewEnvelopeProto(
	id string,
	m dogma.Message,
	times ...time.Time,
) *envelopespec.Envelope {
	env := NewEnvelope(id, m, times...)
	return envelope.MustMarshal(env)
}

// cleanseTime marshals/unmarshals time to strip any internal state that would
// not be transmitted across the network.
func cleanseTime(t *time.Time) {
	if t.IsZero() {
		*t = time.Time{}
		return
	}

	data, err := t.MarshalText()
	if err != nil {
		panic(err)
	}

	err = t.UnmarshalText(data)
	if err != nil {
		panic(err)
	}
}
