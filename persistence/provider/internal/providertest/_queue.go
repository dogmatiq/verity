package providertest

import (
	"context"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/dogma"
	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/envelope"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

func declareMessageQueueTests(
	ctx *context.Context,
	in *In,
	out *Out,
) {
	var (
		dataStore        persistence.DataStore
		queue            persistence.Queue
		envCommand       *envelope.Envelope
		envTimeoutPast   *envelope.Envelope
		envTimeoutFuture *envelope.Envelope
	)

	app := configkit.FromApplication(&dogmafixtures.Application{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<app-name>", "<app-key>")
		},
	})

	ginkgo.Describe("type MessageQueue (interface)", func() {
		ginkgo.BeforeEach(func() {
			var err error
			dataStore, err = out.Provider.Open(*ctx, app, in.Marshaler)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

			queue = dataStore.MessageQueue()

			envCommand = infixfixtures.NewEnvelope(
				"<command>",
				dogmafixtures.MessageA1,
			)

			envTimeoutPast = infixfixtures.NewEnvelope(
				"<timeout-past>",
				dogmafixtures.MessageB1,
				time.Now(),
				time.Now().Add(-1*time.Hour),
			)

			envTimeoutFuture = infixfixtures.NewEnvelope(
				"<timeout-future>",
				dogmafixtures.MessageC1,
				time.Now(),
				time.Now().Add(1*time.Hour),
			)
		})

		ginkgo.AfterEach(func() {
			if dataStore != nil {
				dataStore.Close()
			}
		})

		ginkgo.Describe("func Begin()", func() {
			ginkgo.When("the queue is empty", func() {
				ginkgo.It("blocks", func() {
					ctx, cancel := context.WithTimeout(*ctx, out.AssumeBlockingDuration)
					defer cancel()

					_, _, err := queue.Begin(ctx)
					gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
				})

				ginkgo.When("a message is enqueued", func() {
					waitThenEnqueue := func(env *envelope.Envelope) {
						go func() {
							defer ginkgo.GinkgoRecover()
							time.Sleep(out.AssumeBlockingDuration)
							err := queue.Enqueue(*ctx, env)
							gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						}()
					}

					ginkgo.It("wakes for enqueued command messages", func() {
						waitThenEnqueue(envCommand)

						tx, env, err := queue.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Close()

						gomega.Expect(env).To(gomega.Equal(envCommand))
					})

					ginkgo.It("wakes for enqueued timeout messages that are scheduled in the past", func() {
						waitThenEnqueue(envTimeoutPast)

						tx, env, err := queue.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Close()

						gomega.Expect(env).To(gomega.Equal(envTimeoutPast))
					})

					ginkgo.It("wakes for enqueued timeout messages that are scheduled to occur during the context deadline", func() {
						envTimeoutFuture.ScheduledFor = time.Now().Add(
							out.AssumeBlockingDuration * 2,
						)

						waitThenEnqueue(envTimeoutFuture)

						tx, env, err := queue.Begin(*ctx)
						gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
						defer tx.Close()

						gomega.Expect(env).To(gomega.Equal(envTimeoutFuture))
					})

					ginkgo.It("does not wake for enqueued timeout messages that are scheduled to occur after the context deadline", func() {
						waitThenEnqueue(envTimeoutFuture)

						ctx, cancel := context.WithTimeout(*ctx, out.AssumeBlockingDuration*2)
						defer cancel()

						_, _, err := queue.Begin(ctx)
						gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
					})
				})
			})

			ginkgo.When("the queue is not empty", func() {
				ginkgo.It("returns command messages", func() {
					err := queue.Enqueue(*ctx, envCommand)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					tx, env, err := queue.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envCommand))
				})

				ginkgo.It("returns past-scheduled timeouts", func() {
					err := queue.Enqueue(*ctx, envTimeoutPast)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					tx, env, err := queue.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envTimeoutPast))
				})

				ginkgo.It("returns timeout messages that are scheduled to occur during the context deadline", func() {
					envTimeoutFuture.ScheduledFor = time.Now().Add(
						out.AssumeBlockingDuration,
					)

					err := queue.Enqueue(*ctx, envTimeoutFuture)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					tx, env, err := queue.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envTimeoutFuture))
				})

				ginkgo.It("does not return messages that are scheduled to occur after the context deadline", func() {
					err := queue.Enqueue(*ctx, envTimeoutFuture)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					ctx, cancel := context.WithTimeout(*ctx, out.AssumeBlockingDuration)
					defer cancel()

					_, _, err = queue.Begin(ctx)
					gomega.Expect(err).To(gomega.Equal(context.DeadlineExceeded))
				})

				ginkgo.It("pushes messages that are ready to be handled to the front of the queue", func() {
					err := queue.Enqueue(*ctx, envTimeoutFuture)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					err = queue.Enqueue(*ctx, envCommand)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())

					tx, env, err := queue.Begin(*ctx)
					gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
					defer tx.Close()

					gomega.Expect(env).To(gomega.Equal(envCommand))
				})
			})
		})
	})
}
