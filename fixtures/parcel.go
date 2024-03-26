package fixtures

import (
	"strconv"
	"sync"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/fixtures"
	"github.com/dogmatiq/verity/parcel"
	"github.com/google/uuid"
)

const (
	// DefaultAppKey is the default application key for test envelopes.
	DefaultAppKey = "a96fefa1-2630-467a-b756-db2e428a56fd"

	// DefaultHandlerKey is the default handler key for test envelopes.
	DefaultHandlerKey = "16c7843f-c94f-4fd1-ba80-fd59cab793ff"
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
) *envelopespec.Envelope {
	return NewParcel(id, m, times...).Envelope
}

// NewParcel returns a new parcel containing the given message.
//
// If id is empty, a new UUID is generated.
//
// times can contain up to two elements, the first is the created time, the
// second is the scheduled-for time.
func NewParcel(
	id string,
	m dogma.Message,
	times ...time.Time,
) parcel.Parcel {
	if id == "" {
		id = uuid.NewString()
	}

	var createdAt, scheduledFor time.Time

	switch len(times) {
	case 0:
		createdAt = time.Now()
	case 1:
		createdAt = times[0]
	case 2:
		createdAt = times[0]
		scheduledFor = times[1]
	default:
		panic("too many times specified")
	}

	cleanseTime(&createdAt)
	cleanseTime(&scheduledFor)

	p := parcel.Parcel{
		Envelope: &envelopespec.Envelope{
			MessageId:     id,
			CausationId:   "<cause>",
			CorrelationId: "<correlation>",
			SourceApplication: &envelopespec.Identity{
				Name: "<app-name>",
				Key:  DefaultAppKey,
			},
			SourceHandler: &envelopespec.Identity{
				Name: "<handler-name>",
				Key:  DefaultHandlerKey,
			},
			SourceInstanceId: "<instance>",
			CreatedAt:        marshalkit.MustMarshalEnvelopeTime(createdAt),
			ScheduledFor:     marshalkit.MustMarshalEnvelopeTime(scheduledFor),
			Description:      m.MessageDescription(),
		},
		Message:      m,
		CreatedAt:    createdAt,
		ScheduledFor: scheduledFor,
	}

	marshalkit.MustMarshalMessageIntoEnvelope(fixtures.Marshaler, m, p.Envelope)

	return p
}

// NewPacker returns a parcel packer that uses a deterministic ID sequence and
// clock.
//
// MessageID is a monotonically increasing integer, starting at 0. CreatedAt
// starts at 2000-01-01 00:00:00 UTC and increases by 1 second for each message.
//
// The given roles are valid both as produced and consumed roles.
func NewPacker(roles message.TypeRoles) *parcel.Packer {
	var (
		m   sync.Mutex
		id  int64
		now = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	return &parcel.Packer{
		Application: &envelopespec.Identity{
			Name: "<app-name>",
			Key:  DefaultAppKey,
		},
		Marshaler: fixtures.Marshaler,
		Produced:  roles,
		Consumed:  roles,
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
