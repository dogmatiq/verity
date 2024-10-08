package aggregate_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/marshaler"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/handler/aggregate"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Loader", func() {
	var (
		ctx       context.Context
		dataStore *DataStoreStub
		base      *AggregateRootStub
		loader    *Loader
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		dataStore = NewDataStoreStub()
		DeferCleanup(dataStore.Close)

		base = &AggregateRootStub{}

		loader = &Loader{
			AggregateRepository: dataStore,
			EventRepository:     dataStore,
			Marshaler:           Marshaler,
		}
	})

	Describe("func Load()", func() {
		It("returns an error if the meta-data can not be loaded", func() {
			dataStore.LoadAggregateMetaDataFunc = func(
				context.Context,
				string,
				string,
			) (persistence.AggregateMetaData, error) {
				return persistence.AggregateMetaData{}, errors.New("<error>")
			}

			_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
			Expect(err).To(MatchError("<error>"))
		})

		When("the instance has never existed", func() {
			It("returns an instance with a new meta-data value and the base root", func() {
				inst, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(inst).To(Equal(
					&Instance{
						AggregateMetaData: persistence.AggregateMetaData{
							HandlerKey: DefaultHandlerKey,
							InstanceID: "<instance>",
						},
						Root: base,
					},
				))
			})

			It("does not attempt to load events", func() {
				dataStore.LoadEventsBySourceFunc = func(
					context.Context,
					string,
					string,
					string,
				) (persistence.EventResult, error) {
					return nil, errors.New("<error>")
				}

				_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("the instance exists", func() {
			BeforeEach(func() {
				_, err := dataStore.Persist(
					ctx,
					persistence.Batch{
						persistence.SaveEvent{
							Envelope: NewEnvelope("<event-0>", EventE1),
						},
						persistence.SaveEvent{
							Envelope: NewEnvelope("<event-1>", EventE2),
						},
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey:     DefaultHandlerKey,
								InstanceID:     "<instance>",
								InstanceExists: true,
							},
						},
					},
				)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("returns an instance with the persisted meta-data and the base root", func() {
				inst, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(inst).To(Equal(
					&Instance{
						AggregateMetaData: persistence.AggregateMetaData{
							HandlerKey:     DefaultHandlerKey,
							InstanceID:     "<instance>",
							Revision:       1,
							InstanceExists: true,
						},
						Root: base,
					},
				))
			})

			It("applies historical events to the base root", func() {
				_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(base.AppliedEvents).To(Equal(
					[]dogma.Event{
						EventE1,
						EventE2,
					},
				))
			})

			It("returns an error if the events can not be loaded", func() {
				dataStore.LoadEventsBySourceFunc = func(
					context.Context,
					string,
					string,
					string,
				) (persistence.EventResult, error) {
					return nil, errors.New("<error>")
				}

				_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if one of the historical events can not be unmarshaled", func() {
				m, err := marshaler.New(nil, nil) // an empty marshaler cannot unmarshal anything
				Expect(err).ShouldNot(HaveOccurred())

				loader.Marshaler = m

				_, err = loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
				Expect(err).To(MatchError("no codecs support the 'application/json' media-type"))
			})

			When("the instance has been destroyed", func() {
				BeforeEach(func() {
					_, err := dataStore.Persist(
						ctx,
						persistence.Batch{
							persistence.SaveAggregateMetaData{
								MetaData: persistence.AggregateMetaData{
									HandlerKey:     DefaultHandlerKey,
									InstanceID:     "<instance>",
									Revision:       1,
									InstanceExists: false,
									LastEventID:    "<event-1>",
									BarrierEventID: "<event-1>",
								},
							},
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("does not attempt to load events", func() {
					dataStore.LoadEventsBySourceFunc = func(
						context.Context,
						string,
						string,
						string,
					) (persistence.EventResult, error) {
						return nil, errors.New("<error>")
					}

					_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
					Expect(err).ShouldNot(HaveOccurred())
				})

				When("the instance is subsequently recreated", func() {
					BeforeEach(func() {
						_, err := dataStore.Persist(
							ctx,
							persistence.Batch{
								persistence.SaveEvent{
									Envelope: NewEnvelope("<event-2>", EventE3),
								},
								persistence.SaveAggregateMetaData{
									MetaData: persistence.AggregateMetaData{
										HandlerKey:     DefaultHandlerKey,
										InstanceID:     "<instance>",
										Revision:       2,
										InstanceExists: true,
										LastEventID:    "<event-2>",
										BarrierEventID: "<event-1>",
									},
								},
							},
						)
						Expect(err).ShouldNot(HaveOccurred())
					})

					It("only applies events that were recorded after the destruction", func() {
						_, err := loader.Load(ctx, DefaultHandlerKey, "<instance>", base)
						Expect(err).ShouldNot(HaveOccurred())
						Expect(base.AppliedEvents).To(Equal(
							[]dogma.Event{
								EventE3,
							},
						))
					})
				})
			})
		})
	})
})
