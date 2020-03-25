package providertest

import (
	"context"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/internal/testing/streamtest"
	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/gomega"
)

func declareEventStreamTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	var dataStore persistence.DataStore

	streamtest.Declare(
		func(ctx context.Context, sin streamtest.In) streamtest.Out {
			var err error
			dataStore, err = out.Provider.Open(ctx, sin.Application, sin.Marshaler)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			return streamtest.Out{
				Stream: dataStore.EventStream(),
				Append: func(ctx context.Context, envelopes ...*envelope.Envelope) {
					q := dataStore.MessageQueue()

					err := q.Enqueue(
						ctx,
						infixfixtures.NewEnvelope("<command>", dogmafixtures.MessageX1),
					)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					tx, _, err := q.Begin(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					for _, env := range envelopes {
						err := tx.Append(ctx, env)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					}

					err = tx.Commit(ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
				},
			}
		},
		func() {
			if dataStore != nil {
				dataStore.Close()
			}
		},
	)
}
