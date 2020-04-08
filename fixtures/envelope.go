package fixtures

import (
	"strconv"
	"sync"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/marshalkit/fixtures"
	"github.com/google/uuid"
)

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
	env := NewEnvelope(id, m, times...)
	return envelope.MustMarshal(fixtures.Marshaler, env)
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

// NewPacker returns an envelope packer that uses a deterministic ID sequence
// and clock.
//
// MessageID is a monotonically increasing integer, starting at 0. CreatedAt
// starts at 2000-01-01 00:00:00 UTC and increases by 1 second for each message.
func NewPacker(roles message.TypeRoles) *envelope.Packer {
	var (
		m   sync.Mutex
		id  int64
		now = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	return &envelope.Packer{
		Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
		Roles:       roles,
		GenerateID: func() string {
			m.Lock()
			defer m.Unlock()

			v := strconv.FormatInt(id, 10)
			id++

			return v
		},
		Now: func() time.Time {
			m.Lock()
			defer m.Unlock()

			v := now
			now = now.Add(1 * time.Second)

			return v
		},
	}
}
