package fixtures

import (
	"context"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/pipeline"
	marshalfixtures "github.com/dogmatiq/marshalkit/fixtures"
)

// SessionStub is a test implementation of the pipeline.Session interface.
type SessionStub struct {
	pipeline.Session

	MessageIDFunc    func() string
	FailureCountFunc func() uint
	EnvelopeFunc     func() *envelopespec.Envelope
	MessageFunc      func() (dogma.Message, error)
	TxFunc           func(context.Context) (persistence.ManagedTransaction, error)
	AckFunc          func(context.Context) error
	NackFunc         func(context.Context, time.Time) error
	CloseFunc        func() error
}

// NewPipelineScope returns a new pipeline scope that uses a session stub with
// pre-configured Envelope() and Tx() methods.
func NewPipelineScope(
	env *envelopespec.Envelope,
	ds *DataStoreStub,
) (
	*pipeline.Scope,
	*SessionStub,
	*TransactionStub,
) {
	var tx *TransactionStub

	if ds == nil {
		tx = &TransactionStub{}
	} else {
		t, err := ds.Begin(context.Background())
		if err != nil {
			panic(err)
		}

		tx = t.(*TransactionStub)
	}

	sess := &SessionStub{
		MessageIDFunc: func() string {
			return env.MetaData.MessageId
		},
		EnvelopeFunc: func() *envelopespec.Envelope {
			return env
		},
		MessageFunc: func() (dogma.Message, error) {
			return envelopespec.UnmarshalMessage(marshalfixtures.Marshaler, env)
		},
		TxFunc: func(context.Context) (persistence.ManagedTransaction, error) {
			return tx, nil
		},
	}

	sc := &pipeline.Scope{
		Session:   sess,
		Marshaler: marshalfixtures.Marshaler,
		Logger:    &logging.BufferedLogger{},
	}

	return sc, sess, tx
}

// MessageID returns the ID of the message that started the session.
func (s *SessionStub) MessageID() string {
	if s.MessageIDFunc != nil {
		return s.MessageIDFunc()
	}

	if s.Session != nil {
		return s.Session.MessageID()
	}

	return ""
}

// FailureCount returns the number of times this message has already been
// attempted, not including this attempt.
func (s *SessionStub) FailureCount() uint {
	if s.FailureCountFunc != nil {
		return s.FailureCountFunc()
	}

	if s.Session != nil {
		return s.Session.FailureCount()
	}

	return 0
}

// Envelope returns the message envelope.
func (s *SessionStub) Envelope() *envelopespec.Envelope {
	if s.EnvelopeFunc != nil {
		return s.EnvelopeFunc()
	}

	if s.Session != nil {
		return s.Session.Envelope()
	}

	return nil
}

// Message returns the Dogma message that is to be handled.
func (s *SessionStub) Message() (dogma.Message, error) {
	if s.EnvelopeFunc != nil {
		return s.MessageFunc()
	}

	if s.Session != nil {
		return s.Session.Message()
	}

	return nil, nil
}

// Tx returns the transaction used to persist data within this session.
func (s *SessionStub) Tx(ctx context.Context) (persistence.ManagedTransaction, error) {
	if s.TxFunc != nil {
		return s.TxFunc(ctx)
	}

	if s.Session != nil {
		return s.Session.Tx(ctx)
	}

	return nil, nil
}

// Ack acknowledges successful handling of the message.
//
// It commits the changes performed in the session's transaction.
func (s *SessionStub) Ack(ctx context.Context) error {
	if s.AckFunc != nil {
		return s.AckFunc(ctx)
	}

	if s.Session != nil {
		return s.Session.Ack(ctx)
	}

	return nil
}

// Nack indicates an error while handling the message.
//
// It discards the changes performed in the session's transaction and defers
// handling of the message until n.
func (s *SessionStub) Nack(ctx context.Context, n time.Time) error {
	if s.NackFunc != nil {
		return s.NackFunc(ctx, n)
	}

	if s.Session != nil {
		return s.Session.Nack(ctx, n)
	}

	return nil
}

// Close releases the session.
//
// It must be called regardless of whether Ack() or Nack() are called.
func (s *SessionStub) Close() error {
	if s.CloseFunc != nil {
		return s.CloseFunc()
	}

	if s.Session != nil {
		return s.Session.Close()
	}

	return nil
}
