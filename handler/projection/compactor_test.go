package projection_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/verity/handler/projection"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"golang.org/x/sync/semaphore"
)

var _ = Describe("type Compactor", func() {
	var (
		logger    *logging.BufferedLogger
		handler   *ProjectionMessageHandler
		compactor *Compactor
	)

	BeforeEach(func() {
		logger = &logging.BufferedLogger{}
		handler = &ProjectionMessageHandler{}
		compactor = &Compactor{
			Handler:   handler,
			Interval:  1 * time.Millisecond,
			Timeout:   1 * time.Millisecond,
			Semaphore: semaphore.NewWeighted(1),
			Logger:    logger,
		}
	})

	Describe("func Run()", func() {
		It("compacts immediately", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			handler.CompactFunc = func(
				context.Context,
				dogma.ProjectionCompactScope,
			) error {
				cancel()
				return nil
			}

			err := compactor.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("compacts repeatedly", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			called := true
			handler.CompactFunc = func(
				context.Context,
				dogma.ProjectionCompactScope,
			) error {
				if called {
					cancel()
				}

				called = true
				return nil
			}

			err := compactor.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns an error if the context is canceled while waiting for the semaphore", func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			err := compactor.Semaphore.Acquire(ctx, 1)
			Expect(err).ShouldNot(HaveOccurred())
			defer compactor.Semaphore.Release(1)

			go func() {
				time.Sleep(100 * time.Millisecond)
				cancel()
			}()

			err = compactor.Run(ctx)
			Expect(err).To(Equal(context.Canceled))
		})

		It("returns an error if compaction fails", func() {
			handler.CompactFunc = func(
				context.Context,
				dogma.ProjectionCompactScope,
			) error {
				return errors.New("<error>")
			}

			err := compactor.Run(context.Background())
			Expect(err).To(MatchError("<error>"))
		})

		It("does not return an error if compaction times out", func() {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			handler.CompactFunc = func(
				compactCtx context.Context,
				_ dogma.ProjectionCompactScope,
			) error {
				<-compactCtx.Done()
				cancel() // cancel the parent ctx only after the compactCtx is already done
				return compactCtx.Err()
			}

			err := compactor.Run(ctx)
			Expect(err).To(Equal(context.Canceled)) // canceled, not deadline exceeded
		})
	})
})
