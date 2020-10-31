package providertest

import (
	"sort"
	"sync"

	dogmafixtures "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/envelopespec"
	infixfixtures "github.com/dogmatiq/infix/fixtures"
	"github.com/dogmatiq/infix/persistence"
	"github.com/jmalloc/gomegax"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareEventOperationTests declares a functional test-suite for
// persistence operations related to events.
func declareEventOperationTests(tc *TestContext) {
	ginkgo.Context("event operations", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()

			env0, env1, env2 *envelopespec.Envelope
			filter           map[string]struct{}
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()

			env0 = infixfixtures.NewEnvelope("<message-0>", dogmafixtures.MessageA1)
			env1 = infixfixtures.NewEnvelope("<message-1>", dogmafixtures.MessageB1)
			env2 = infixfixtures.NewEnvelope("<message-2>", dogmafixtures.MessageC1)

			filter = map[string]struct{}{
				env0.PortableName: {},
				env1.PortableName: {},
				env2.PortableName: {},
			}
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("type persistence.SaveEvent", func() {
			ginkgo.It("saves the event", func() {
				persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: env0,
					},
					persistence.SaveEvent{
						Envelope: env1,
					},
				)

				events := loadEventsByType(tc.Context, dataStore, filter, 0)
				gomega.Expect(events).To(gomegax.EqualX(
					[]persistence.Event{
						{
							Offset:   0,
							Envelope: env0,
						},
						{
							Offset:   1,
							Envelope: env1,
						},
					},
				))
			})

			ginkgo.It("has a corresponding offset in the result", func() {
				res := persist(
					tc.Context,
					dataStore,
					persistence.SaveEvent{
						Envelope: env0,
					},
					persistence.SaveEvent{
						Envelope: env1,
					},
				)

				gomega.Expect(res.EventOffsets).To(
					gomega.Equal(map[string]uint64{
						env0.GetMessageId(): 0,
						env1.GetMessageId(): 1,
					}),
				)
			})

			ginkgo.It("serializes operations from concurrent persist calls", func() {
				var (
					g      sync.WaitGroup
					m      sync.Mutex
					expect []persistence.Event
				)

				fn := func(env *envelopespec.Envelope) {
					defer ginkgo.GinkgoRecover()
					defer g.Done()

					res := persist(
						tc.Context,
						dataStore,
						persistence.SaveEvent{
							Envelope: env,
						},
					)

					m.Lock()
					expect = append(
						expect,
						persistence.Event{
							Offset:   res.EventOffsets[env.GetMessageId()],
							Envelope: env,
						},
					)
					m.Unlock()
				}

				g.Add(3)
				go fn(env0)
				go fn(env1)
				go fn(env2)
				g.Wait()

				// Sort the expected slice, as the appends in each goroutine
				// could be out-of-sync with the saves.
				sort.Slice(
					expect,
					func(i, j int) bool {
						return expect[i].Offset < expect[j].Offset
					},
				)

				events := loadEventsByType(tc.Context, dataStore, filter, 0)
				gomega.Expect(events).To(gomegax.EqualX(expect))
			})
		})
	})
}
