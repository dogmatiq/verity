package process_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/handler/process"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Loader", func() {
	var (
		ctx       context.Context
		dataStore *DataStoreStub
		base      *ProcessRootStub
		loader    *Loader
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		dataStore = NewDataStoreStub()
		DeferCleanup(dataStore.Close)

		base = &ProcessRootStub{}

		loader = &Loader{
			Repository: dataStore,
		}
	})

	Describe("func Load()", func() {
		It("returns an error if the instance can not be loaded", func() {
			dataStore.LoadProcessInstanceFunc = func(
				context.Context,
				string,
				string,
			) (persistence.ProcessInstance, error) {
				return persistence.ProcessInstance{}, errors.New("<error>")
			}

			_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
			Expect(err).To(MatchError("<error>"))
		})

		When("the instance does not exist", func() {
			It("returns an instance with a new instance value and the base root", func() {
				inst, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(inst).To(Equal(
					&Instance{
						ProcessInstance: persistence.ProcessInstance{
							HandlerKey: DefaultHandlerKey,
							InstanceID: "<instance>",
						},
						Root: base,
					},
				))
			})
		})

		When("the instance exists", func() {
			When("the packet is empty", func() {
				BeforeEach(func() {
					_, err := dataStore.Persist(
						ctx,
						persistence.Batch{
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: DefaultHandlerKey,
									InstanceID: "<instance>",
								},
							},
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("returns an instance with the persisted instance data and a stateless root", func() {
					inst, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(inst).To(Equal(
						&Instance{
							ProcessInstance: persistence.ProcessInstance{
								HandlerKey: DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   1,
							},
							Root: dogma.StatelessProcessRoot,
						},
					))
				})
			})

			When("the packet is not empty", func() {
				var data []byte

				BeforeEach(func() {
					base.Value = "<value>"

					var err error
					data, err = base.MarshalBinary()
					Expect(err).ShouldNot(HaveOccurred())

					_, err = dataStore.Persist(
						ctx,
						persistence.Batch{
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: DefaultHandlerKey,
									InstanceID: "<instance>",
									Data:       data,
								},
							},
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("unmarshals the process state", func() {
					inst, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(inst).To(Equal(
						&Instance{
							ProcessInstance: persistence.ProcessInstance{
								HandlerKey: DefaultHandlerKey,
								InstanceID: "<instance>",
								Revision:   1,
								Data:       data,
							},
							Root: base,
						},
					))
				})

				It("returns an error if the state can not be unmarshaled", func() {
					// TODO

					_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
					Expect(err).To(MatchError("no codecs support the 'application/json' media-type"))
				})
			})
		})

	})
})
