package fixtures

import (
	"strconv"
	"sync"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/envelopespec"
	"github.com/dogmatiq/verity/parcel"
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
		id = uuid.New().String()
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
				Key:  "<app-key>",
			},
			SourceHandler: &envelopespec.Identity{
				Name: "<handler-name>",
				Key:  "<handler-key>",
			},
			SourceInstanceId: "<instance>",
			CreatedAt:        envelopespec.MarshalTime(createdAt),
			ScheduledFor:     envelopespec.MarshalTime(scheduledFor),
			Description:      dogma.DescribeMessage(m),
		},
		Message:      m,
		CreatedAt:    createdAt,
		ScheduledFor: scheduledFor,
	}

	envelopespec.MarshalMessage(fixtures.Marshaler, m, p.Envelope)

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
			Key:  "<app-key>",
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
