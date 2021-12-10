package aggregate_test

import (
	"context"
	"errors"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	"time"

	. "github.com/dogmatiq/configkit/fixtures"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/interopspec/envelopespec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/handler"
	. "github.com/dogmatiq/verity/handler/aggregate"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Adaptor", func() {
	var (
		ctx       context.Context
		cancel    context.CancelFunc
		dataStore *DataStoreStub
		upstream  *AggregateMessageHandler
		packer    *parcel.Packer
		logger    *logging.BufferedLogger
		work      *UnitOfWorkStub
		cause     parcel.Parcel
		adaptor   *Adaptor
	)

	BeforeEach(func() {
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)

		dataStore = NewDataStoreStub()

		dataStore.LoadAggregateMetaDataFunc = func(
			_ context.Context,
			hk, id string,
		) (persistence.AggregateMetaData, error) {
			return persistence.AggregateMetaData{
				HandlerKey: hk,
				InstanceID: id,
			}, nil
		}

		upstream = &AggregateMessageHandler{
			ConfigureFunc: func(c dogma.AggregateConfigurer) {
				c.Identity("<aggregate-name>", "<aggregate-key>")
				c.ConsumesCommandType(MessageC{})
				c.ProducesEventType(MessageE{})
			},
			RouteCommandToInstanceFunc: func(m dogma.Message) string {
				return "<instance>"
			},
		}

		packer = NewPacker(
			message.TypeRoles{
				MessageCType: message.CommandRole,
				MessageEType: message.EventRole,
			},
		)

		logger = &logging.BufferedLogger{}

		work = &UnitOfWorkStub{}

		cause = NewParcel("<consume>", MessageC1)

		adaptor = &Adaptor{
			Identity: &envelopespec.Identity{
				Name: "<aggregate-name>",
				Key:  "<aggregate-key>",
			},
			Handler: upstream,
			Loader: &Loader{
				AggregateRepository: dataStore,
				EventRepository:     dataStore,
				Marshaler:           Marshaler,
			},
			Marshaler:   Marshaler,
			Packer:      packer,
			LoadTimeout: 1 * time.Second,
			Logger:      logger,
		}
	})

	AfterEach(func() {
		dataStore.Close()
		cancel()
	})

	Describe("func HandleMessage()", func() {
		It("forwards the message to the handler", func() {
			called := false
			upstream.HandleCommandFunc = func(
				_ dogma.AggregateRoot,
				_ dogma.AggregateCommandScope,
				m dogma.Message,
			) {
				called = true
				Expect(m).To(Equal(MessageC1))
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("makes the instance ID available via the scope ", func() {
			upstream.HandleCommandFunc = func(
				_ dogma.AggregateRoot,
				s dogma.AggregateCommandScope,
				_ dogma.Message,
			) {
				Expect(s.InstanceID()).To(Equal("<instance>"))
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns an error if the instance can not be loaded", func() {
			dataStore.LoadAggregateMetaDataFunc = func(
				context.Context,
				string,
				string,
			) (persistence.AggregateMetaData, error) {
				return persistence.AggregateMetaData{}, errors.New("<error>")
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).To(MatchError("<error>"))
		})

		It("panics if the handler routes the message to an empty instance ID", func() {
			upstream.RouteCommandToInstanceFunc = func(dogma.Message) string {
				return ""
			}

			Expect(func() {
				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			}).To(PanicWith("*fixtures.AggregateMessageHandler.RouteCommandToInstance() returned an empty instance ID while routing a fixtures.MessageC command"))
		})

		It("panics if the handler returns a nil root", func() {
			upstream.NewFunc = func() dogma.AggregateRoot {
				return nil
			}

			Expect(func() {
				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			}).To(PanicWith("*fixtures.AggregateMessageHandler.New() returned nil"))
		})

		When("an event is recorded", func() {
			It("saves the event and updates the aggregate meta-data", func() {
				upstream.HandleCommandFunc = func(
					_ dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.RecordEvent(MessageE1)
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(work.Events).To(EqualX(
					[]parcel.Parcel{
						{
							Envelope: &envelopespec.Envelope{
								MessageId:         "0",
								CausationId:       "<consume>",
								CorrelationId:     "<correlation>",
								SourceApplication: packer.Application,
								SourceHandler:     adaptor.Identity,
								SourceInstanceId:  "<instance>",
								CreatedAt:         "2000-01-01T00:00:00Z",
								Description:       "{E1}",
								PortableName:      MessageEPortableName,
								MediaType:         MessageE1Packet.MediaType,
								Data:              MessageE1Packet.Data,
							},
							Message:   MessageE1,
							CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
						},
					},
				))

				Expect(work.Operations).To(EqualX(
					[]persistence.Operation{
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey:     "<aggregate-key>",
								InstanceID:     "<instance>",
								InstanceExists: true,
								LastEventID:    "0", // deterministic ID from the packer
							},
						},
					},
				))
			})

			It("applies the event to the aggregate root", func() {
				upstream.HandleCommandFunc = func(
					x dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.RecordEvent(MessageE1)

					r := x.(*AggregateRoot)
					Expect(r.AppliedEvents).To(Equal(
						[]dogma.Message{
							MessageE1,
						},
					))
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("logs about the event", func() {
				upstream.HandleCommandFunc = func(
					_ dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.RecordEvent(MessageE1)
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    MessageE ● {E1}",
					},
				))
			})

			It("returns an error if the aggregate root can not be marshaled", func() {
				upstream.HandleCommandFunc = func(
					x dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.RecordEvent(MessageE1)

					r := x.(*AggregateRoot)
					Expect(r.AppliedEvents).To(Equal(
						[]dogma.Message{
							MessageE1,
						},
					))
				}

				dataStore.LoadAggregateMetaDataFunc = func(
					_ context.Context,
					hk, id string,
				) (persistence.AggregateMetaData, error) {
					return persistence.AggregateMetaData{
						HandlerKey: hk,
						InstanceID: id,
						Revision:   1000,
					}, nil
				}

				adaptor.Marshaler = &codec.Marshaler{} // an empty marshaler cannot marshal anything

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).To(MatchError("no codecs support the '*fixtures.AggregateRoot' type"))
			})

			It("updates the snapshot if interval revision", func() {
				upstream.HandleCommandFunc = func(
					x dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.RecordEvent(MessageE1)

					r := x.(*AggregateRoot)
					Expect(r.AppliedEvents).To(Equal(
						[]dogma.Message{
							MessageE1,
						},
					))
				}

				dataStore.LoadAggregateMetaDataFunc = func(
					_ context.Context,
					hk, id string,
				) (persistence.AggregateMetaData, error) {
					return persistence.AggregateMetaData{
						HandlerKey: hk,
						InstanceID: id,
						Revision:   1000,
					}, nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(work.Operations).To(EqualX(
					[]persistence.Operation{
						persistence.SaveAggregateMetaData{
							MetaData: persistence.AggregateMetaData{
								HandlerKey:     "<aggregate-key>",
								InstanceID:     "<instance>",
								InstanceExists: true,
								LastEventID:    "0",
								Revision:       1000,
								BarrierEventID: "",
							},
						},
						persistence.SaveAggregateSnapshot{
							Snapshot: persistence.AggregateSnapshot{
								HandlerKey: "<aggregate-key>",
								InstanceID: "<instance>",
								Version:    "0",
								Packet: marshalkit.Packet{
									MediaType: "application/json; type=AggregateRoot",
									Data:      []byte(`{"AppliedEvents":[{"Value":"E1"}]}`),
								},
							},
						},
					},
				))
			})
		})

		When("a message is logged via the scope", func() {
			BeforeEach(func() {
				upstream.HandleCommandFunc = func(
					_ dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.Log("format %s", "<value>")
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("logs using the standard format", func() {
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    MessageC ● format <value>",
					},
				))
			})
		})

		It("returns an error if the deadline is exceeded while acquiring the cache record", func() {
			_, err := adaptor.Cache.Acquire(ctx, &UnitOfWorkStub{}, "<instance>")
			Expect(err).ShouldNot(HaveOccurred())

			ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
			defer cancel()

			err = adaptor.HandleMessage(ctx, work, cause)
			Expect(err).To(Equal(context.DeadlineExceeded))
		})

		It("retains the cache record if the unit-of-work is successful", func() {
			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())

			work.Succeed(handler.Result{})

			rec, err := adaptor.Cache.Acquire(ctx, &UnitOfWorkStub{}, "<instance>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rec.Instance).NotTo(BeNil())
		})

		It("does not retain the cache record if the unit-of-work is unsuccessful", func() {
			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())

			work.Fail(errors.New("<error>"))

			rec, err := adaptor.Cache.Acquire(ctx, &UnitOfWorkStub{}, "<instance>")
			Expect(err).ShouldNot(HaveOccurred())
			Expect(rec.Instance).To(BeNil())
		})

		When("the instance does not exist", func() {
			It("allows access to an empty root", func() {
				upstream.HandleCommandFunc = func(
					x dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					r := x.(*AggregateRoot)
					Expect(r.AppliedEvents).To(BeEmpty())
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("does not panic if the instance is destroyed", func() {
				upstream.HandleCommandFunc = func(
					_ dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.Destroy()
				}

				Expect(func() {
					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
				}).NotTo(Panic())
			})
		})

		When("the instance already exists", func() {
			BeforeEach(func() {
				upstream.HandleCommandFunc = func(
					_ dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					s.RecordEvent(MessageE1)
					s.RecordEvent(MessageE2)
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				work.Succeed(handler.Result{})
				work = &UnitOfWorkStub{}

				upstream.HandleCommandFunc = nil
			})

			It("provides a root with the correct state", func() {
				upstream.HandleCommandFunc = func(
					x dogma.AggregateRoot,
					s dogma.AggregateCommandScope,
					_ dogma.Message,
				) {
					r := x.(*AggregateRoot)
					Expect(r.AppliedEvents).To(Equal(
						[]dogma.Message{
							MessageE1,
							MessageE2,
						},
					))
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			When("the instance is destroyed", func() {
				It("causes RecordEvent() to negate the destroy", func() {
					upstream.HandleCommandFunc = func(
						_ dogma.AggregateRoot,
						s dogma.AggregateCommandScope,
						_ dogma.Message,
					) {
						s.Destroy()
						s.RecordEvent(MessageE3)
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(work.Operations).To(EqualX(
						[]persistence.Operation{
							persistence.SaveAggregateMetaData{
								MetaData: persistence.AggregateMetaData{
									HandlerKey:     "<aggregate-key>",
									InstanceID:     "<instance>",
									Revision:       1,
									InstanceExists: true,
									LastEventID:    "2", // deterministic ID from the packer
									BarrierEventID: "",  // must not be set
								},
							},
						},
					))
				})

				It("updates the barrier event on the aggregate meta-data", func() {
					upstream.HandleCommandFunc = func(
						_ dogma.AggregateRoot,
						s dogma.AggregateCommandScope,
						_ dogma.Message,
					) {
						s.RecordEvent(MessageE3)
						s.Destroy()
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(work.Operations).To(EqualX(
						[]persistence.Operation{
							persistence.SaveAggregateMetaData{
								MetaData: persistence.AggregateMetaData{
									HandlerKey:     "<aggregate-key>",
									InstanceID:     "<instance>",
									Revision:       1,
									InstanceExists: false,
									LastEventID:    "2", // deterministic ID from the packer
									BarrierEventID: "2", // deterministic ID from the packer
								},
							},
							persistence.RemoveAggregateSnapshot{
								Snapshot: persistence.AggregateSnapshot{
									HandlerKey: "<aggregate-key>",
									InstanceID: "<instance>",
								},
							},
						},
					))
				})

				It("does not require an event to be recorded", func() {
					upstream.HandleCommandFunc = func(
						_ dogma.AggregateRoot,
						s dogma.AggregateCommandScope,
						_ dogma.Message,
					) {
						s.Destroy()
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(work.Operations).To(EqualX(
						[]persistence.Operation{
							persistence.SaveAggregateMetaData{
								MetaData: persistence.AggregateMetaData{
									HandlerKey:     "<aggregate-key>",
									InstanceID:     "<instance>",
									Revision:       1,
									InstanceExists: false,
									LastEventID:    "1", // deterministic ID from the packer
									BarrierEventID: "1", // deterministic ID from the packer
								},
							},
							persistence.RemoveAggregateSnapshot{
								Snapshot: persistence.AggregateSnapshot{
									HandlerKey: "<aggregate-key>",
									InstanceID: "<instance>",
								},
							},
						},
					))
				})
			})
		})

		It("does not reset the root state when destroyed", func() {
			upstream.HandleCommandFunc = func(
				x dogma.AggregateRoot,
				s dogma.AggregateCommandScope,
				_ dogma.Message,
			) {
				s.RecordEvent(MessageE1)
				s.Destroy()

				r := x.(*AggregateRoot)
				Expect(r.AppliedEvents).To(Equal(
					[]dogma.Message{
						MessageE1,
					},
				))
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})
})
