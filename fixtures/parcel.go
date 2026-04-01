package fixtures

import (
	"reflect"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/verity/parcel"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// DefaultAppKey is the default application key for test envelopes.
	DefaultAppKey = "a96fefa1-2630-467a-b756-db2e428a56fd"

	// DefaultHandlerKey is the default handler key for test envelopes.
	DefaultHandlerKey = "16c7843f-c94f-4fd1-ba80-fd59cab793ff"

	// DefaultCausationID is the default value to use for the causation ID in
	// test envelopes.
	DefaultCausationID = "f5a11411-fc1e-435a-b084-a7c224816768"

	// DefaultCorrelationID is the default value to use for the correlation ID
	// in test envelopes.
	DefaultCorrelationID = "2a211190-6de2-4e24-9dd8-478d357372f2"
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
) *envelopepb.Envelope {
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
	var messageID *uuidpb.UUID

	if id == "" {
		messageID = uuidpb.Generate()
	} else {
		messageID = uuidpb.MustParse(id)
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

	data, err := m.MarshalBinary()
	if err != nil {
		panic(err)
	}

	mt, ok := dogma.RegisteredMessageTypeOf(m)
	if !ok {
		panic("unregistered message type: " + reflect.TypeOf(m).String())
	}

	env := &envelopepb.Envelope{
		MessageId:     messageID,
		CausationId:   uuidpb.MustParse(DefaultCausationID),
		CorrelationId: uuidpb.MustParse(DefaultCorrelationID),
		SourceApplication: &identitypb.Identity{
			Name: "<app-name>",
			Key:  uuidpb.MustParse(DefaultAppKey),
		},
		SourceHandler: &identitypb.Identity{
			Name: "<handler-name>",
			Key:  uuidpb.MustParse(DefaultHandlerKey),
		},
		SourceInstanceId: "<instance>",
		CreatedAt:        timestamppb.New(createdAt),
		Description:      m.MessageDescription(),
		TypeId:           uuidpb.MustParse(mt.ID()),
		Data:             data,
	}

	if !scheduledFor.IsZero() {
		env.ScheduledFor = timestamppb.New(scheduledFor)
	}

	p := parcel.Parcel{
		Envelope:     env,
		Message:      m,
		CreatedAt:    createdAt,
		ScheduledFor: scheduledFor,
	}

	return p
}

// NewPacker returns a parcel packer that uses a deterministic ID sequence and
// clock.
//
// MessageID is a monotonically increasing integer, starting at 0. CreatedAt
// starts at 2000-01-01 00:00:00 UTC and increases by 1 second for each message.
//
// The given types are valid both as produced and consumed messages.
func NewPacker(types ...message.Type) *parcel.Packer {
	var (
		m   sync.Mutex
		id  uint64
		now = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	typeSet := sets.New(types...)

	return &parcel.Packer{
		Application: identitypb.MustParse("<app-name>", DefaultAppKey),
		Produced:    typeSet,
		Consumed:    typeSet,
		GenerateID: func() *uuidpb.UUID {
			m.Lock()
			defer m.Unlock()

			id++

			return &uuidpb.UUID{
				Lower: id,
			}
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
