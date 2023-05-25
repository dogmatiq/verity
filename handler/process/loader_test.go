package process_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/handler/process"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Loader", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		dataStore *DataStoreStub
		base      *ProcessRoot
		loader    *Loader
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		dataStore = NewDataStoreStub()

		base = &ProcessRoot{}

		loader = &Loader{
			Repository: dataStore,
			Marshaler:  Marshaler,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
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
				var packet marshalkit.Packet

				BeforeEach(func() {
					base.Value = "<value>"

					var err error
					packet, err = Marshaler.Marshal(base)
					Expect(err).ShouldNot(HaveOccurred())

					_, err = dataStore.Persist(
						ctx,
						persistence.Batch{
							persistence.SaveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: DefaultHandlerKey,
									InstanceID: "<instance>",
									Packet:     packet,
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
								Packet:     packet,
							},
							Root: base,
						},
					))
				})

				It("returns an error if the state can not be unmarshaled", func() {
					loader.Marshaler = &codec.Marshaler{} // an empty marshaler cannot unmarshal anything
					_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
					Expect(err).To(MatchError("no codecs support the 'application/json' media-type"))
				})
			})
		})

	})
})
