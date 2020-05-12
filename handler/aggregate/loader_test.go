package aggregate_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/handler/aggregate"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/infix/persistence/subsystem/aggregatestore"
	"github.com/dogmatiq/infix/persistence/subsystem/eventstore"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Loader", func() {
	var (
		ctx           context.Context
		cancel        context.CancelFunc
		dataStore     *DataStoreStub
		aggregateRepo *AggregateStoreRepositoryStub
		eventRepo     *EventStoreRepositoryStub
		applied       []dogma.Message
		root          *AggregateRoot
		loader        *Loader
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		dataStore = NewDataStoreStub()
		aggregateRepo = dataStore.AggregateStoreRepository().(*AggregateStoreRepositoryStub)
		eventRepo = dataStore.EventStoreRepository().(*EventStoreRepositoryStub)

		applied = nil
		root = &AggregateRoot{
			ApplyEventFunc: func(m dogma.Message, _ interface{}) {
				applied = append(applied, m)
			},
		}

		loader = &Loader{
			AggregateStore: aggregateRepo,
			EventStore:     eventRepo,
			Marshaler:      Marshaler,
		}
	})

	AfterEach(func() {
		if dataStore != nil {
			dataStore.Close()
		}

		cancel()
	})

	Describe("func Load()", func() {
		It("returns an error if the meta-data can not be loaded", func() {
			aggregateRepo.LoadMetaDataFunc = func(
				context.Context,
				string,
				string,
			) (*aggregatestore.MetaData, error) {
				return nil, errors.New("<error>")
			}

			_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
			Expect(err).To(MatchError("<error>"))
		})

		When("the instance has never existed", func() {
			It("returns a new meta-data value", func() {
				md, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(md).To(Equal(
					&aggregatestore.MetaData{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				))
			})

			It("does not attempt to load events", func() {
				eventRepo.QueryEventsFunc = func(
					context.Context,
					eventstore.Query,
				) (eventstore.Result, error) {
					return nil, errors.New("<error>")
				}

				_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("the instance exists", func() {
			var metadata *aggregatestore.MetaData

			BeforeEach(func() {
				metadata = &aggregatestore.MetaData{
					HandlerKey: "<handler-key>",
					InstanceID: "<instance>",
				}

				_, err := persistence.WithTransaction(
					ctx,
					dataStore,
					func(tx persistence.ManagedTransaction) error {
						if _, err := tx.SaveEvent(ctx, NewEnvelope("<event-0>", MessageE1)); err != nil {
							return err
						}

						if _, err := tx.SaveEvent(ctx, NewEnvelope("<event-1>", MessageE2)); err != nil {
							return err
						}

						metadata.InstanceExists = true

						return tx.SaveAggregateMetaData(ctx, metadata)
					},
				)
				Expect(err).ShouldNot(HaveOccurred())
				metadata.Revision++
			})

			It("returns the persisted meta-data", func() {
				md, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(md).To(Equal(metadata))
			})

			It("applies historical events", func() {
				_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(applied).To(Equal(
					[]dogma.Message{
						MessageE1,
						MessageE2,
					},
				))
			})

			It("returns an error if the events can not be loaded", func() {
				eventRepo.LoadEventsBySourceFunc = func(
					context.Context,
					string,
					string,
					string,
				) (eventstore.Result, error) {
					return nil, errors.New("<error>")
				}

				_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
				Expect(err).To(MatchError("<error>"))
			})

			It("returns an error if one of the historical events can not be unmarshaled", func() {
				loader.Marshaler = &codec.Marshaler{} // an empty marshaler cannot unmarshal anything
				_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
				Expect(err).To(MatchError("no codecs support the 'application/json' media-type"))
			})

			When("the aggregate is stateless", func() {
				root := dogma.StatelessAggregateRoot // shadow

				It("does not attempt to load events", func() {
					eventRepo.QueryEventsFunc = func(
						context.Context,
						eventstore.Query,
					) (eventstore.Result, error) {
						return nil, errors.New("<error>")
					}

					_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})

			When("the instance has been destroyed", func() {
				BeforeEach(func() {
					_, err := persistence.WithTransaction(
						ctx,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							metadata.LastDestroyedBy = "<event-1>"
							return tx.SaveAggregateMetaData(ctx, metadata)
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
					metadata.Revision++
				})

				It("does not attempt to load events if the range is empty", func() {
					eventRepo.QueryEventsFunc = func(
						context.Context,
						eventstore.Query,
					) (eventstore.Result, error) {
						return nil, errors.New("<error>")
					}

					_, err := loader.Load(ctx, "<handler-key>", "<instance>", root)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("only loads events that were recorded after the destruction", func() {
					_, err := persistence.WithTransaction(
						ctx,
						dataStore,
						func(tx persistence.ManagedTransaction) error {
							if _, err := tx.SaveEvent(ctx, NewEnvelope("<event-2>", MessageE3)); err != nil {
								return err
							}

							return tx.SaveAggregateMetaData(ctx, metadata)
						},
					)
					Expect(err).ShouldNot(HaveOccurred())
					metadata.Revision++

					_, err = loader.Load(ctx, "<handler-key>", "<instance>", root)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(applied).To(Equal(
						[]dogma.Message{
							MessageE3,
						},
					))
				})
			})
		})
	})
})
