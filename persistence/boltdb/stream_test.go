package boltdb_test

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/persistence"
	. "github.com/dogmatiq/infix/persistence/boltdb"
	"github.com/dogmatiq/infix/persistence/internal/streamtest"
	"github.com/dogmatiq/marshalkit"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"go.etcd.io/bbolt"
)

var _ = Describe("type Stream (standard test suite)", func() {
	var (
		tmpfile string
		db      *bbolt.DB
		stream  *Stream
	)

	streamtest.Declare(
		func(ctx context.Context, m marshalkit.Marshaler) persistence.Stream {
			f, err := ioutil.TempFile("", "*.boltdb")
			Expect(err).ShouldNot(HaveOccurred())
			f.Close()
			tmpfile = f.Name()

			db, err = bbolt.Open(tmpfile, 0600, bbolt.DefaultOptions)
			Expect(err).ShouldNot(HaveOccurred())

			stream = &Stream{
				DB:        db,
				Marshaler: m,
				BucketPath: [][]byte{
					[]byte("path"),
					[]byte("to"),
					[]byte("bucket"),
				},
			}

			return stream
		},
		func() {
			if db != nil {
				db.Close()
			}

			if tmpfile != "" {
				os.Remove(tmpfile)
			}
		},
		func(ctx context.Context, envelopes ...*envelope.Envelope) {
			err := db.Update(func(tx *bbolt.Tx) error {
				_, err := stream.Append(tx, envelopes...)
				return err
			})
			Expect(err).ShouldNot(HaveOccurred())
		},
	)
})
