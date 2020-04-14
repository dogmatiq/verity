package fixtures

import (
	"strconv"
	"sync"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/parcel"
	"github.com/dogmatiq/marshalkit/fixtures"
)

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
) *parcel.Parcel {
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

	return &parcel.Parcel{
		Envelope: &envelopespec.Envelope{
			MetaData: &envelopespec.MetaData{
				MessageId:     id,
				CausationId:   "<cause>",
				CorrelationId: "<correlation>",
				Source: &envelopespec.Source{
					Application: &envelopespec.Identity{
						Key:  "<app-name>",
						Name: "<app-key>",
					},
					Handler: &envelopespec.Identity{
						Key:  "<handler-name>",
						Name: "<handler-key>",
					},
					InstanceId: "<instance>",
				},
				CreatedAt:    envelope.MarshalTime(createdAt),
				ScheduledFor: envelope.MarshalTime(scheduledFor),
			},
		},
		Message:      m,
		ScheduledFor: scheduledFor,
	}
}

// NewPacker returns an parce packer that uses a deterministic ID sequence and
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
