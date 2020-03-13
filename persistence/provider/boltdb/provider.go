package boltdb

import (
	"context"
	"os"
	"sync"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/linger"
	"github.com/dogmatiq/marshalkit"
	"go.etcd.io/bbolt"
)

// Provider is a implementation of provider.Provider for BoltDB.
type Provider struct {
	File    string
	Mode    os.FileMode
	Options *bbolt.Options

	db        *bbolt.DB
	marshaler marshalkit.Marshaler
	streams   sync.Map
}

// Initialize prepare the provider for use.
func (p *Provider) Initialize(ctx context.Context, m marshalkit.Marshaler) error {
	file := p.File
	if file == "" {
		file = "/var/run/infix.boltdb"
	}

	mode := p.Mode
	if mode == 0 {
		mode = 0600
	}

	opts := p.Options

	if timeout, ok := linger.FromContextDeadline(ctx); ok {
		if opts == nil {
			opts = &bbolt.Options{}
		}

		if timeout < p.Options.Timeout || p.Options.Timeout == 0 {
			p.Options.Timeout = timeout
		}
	}

	db, err := bbolt.Open(file, mode, opts)
	if err != nil {
		return err
	}

	p.db = db
	p.marshaler = m

	return nil
}

// EventStream returns the event stream for the given application.
func (p *Provider) EventStream(ctx context.Context, app configkit.Identity) (persistence.Stream, error) {
	stream := &Stream{
		DB:        p.db,
		Marshaler: p.marshaler,
		BucketPath: [][]byte{
			[]byte(app.Key),
			[]byte("eventstream"),
		},
	}

	v, _ := p.streams.LoadOrStore(app.Key, stream)
	return v.(persistence.Stream), nil
}
