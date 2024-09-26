package process_test

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/dogmatiq/verity/fixtures"
	"github.com/dogmatiq/verity/handler"
	. "github.com/dogmatiq/verity/handler/process"
	"github.com/dogmatiq/verity/parcel"
	"github.com/dogmatiq/verity/persistence"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Adaptor", func() {
	var (
		ctx       context.Context
		dataStore *DataStoreStub
		upstream  *ProcessMessageHandlerStub
		packer    *parcel.Packer
		logger    *logging.BufferedLogger
		work      *UnitOfWorkStub
		cause     parcel.Parcel
		adaptor   *Adaptor
	)

	BeforeEach(func() {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		DeferCleanup(cancel)

		dataStore = NewDataStoreStub()
		DeferCleanup(dataStore.Close)

		dataStore.LoadProcessInstanceFunc = func(
			_ context.Context,
			hk, id string,
		) (persistence.ProcessInstance, error) {
			return persistence.ProcessInstance{
				HandlerKey: hk,
				InstanceID: id,
			}, nil
		}

		upstream = &ProcessMessageHandlerStub{
			ConfigureFunc: func(c dogma.ProcessConfigurer) {
				c.Identity("<process-name>", "2ae0b937-e806-4e70-9b23-f36298f68973")
				c.Routes(
					dogma.HandlesEvent[EventStub[TypeE]](),
					dogma.ExecutesCommand[CommandStub[TypeC]](),
					dogma.SchedulesTimeout[TimeoutStub[TypeT]](),
				)
			},
			RouteEventToInstanceFunc: func(_ context.Context, m dogma.Event) (string, bool, error) {
				return "<instance>", true, nil
			},
		}

		packer = NewPacker(
			message.TypeRoles{
				message.TypeFor[CommandStub[TypeC]](): message.CommandRole,
				message.TypeFor[EventStub[TypeE]]():   message.EventRole,
				message.TypeFor[TimeoutStub[TypeT]](): message.TimeoutRole,
			},
		)

		logger = &logging.BufferedLogger{}

		work = &UnitOfWorkStub{}

		cause = NewParcel("<consume>", EventE1)

		adaptor = &Adaptor{
			Identity: &envelopespec.Identity{
				Name: "<process-name>",
				Key:  "2ae0b937-e806-4e70-9b23-f36298f68973",
			},
			Handler: upstream,
			Loader: &Loader{
				Repository: dataStore,
				Marshaler:  Marshaler,
			},
			Marshaler:   Marshaler,
			Packer:      packer,
			LoadTimeout: 1 * time.Second,
			Logger:      logger,
		}
	})

	Describe("func HandleMessage()", func() {
		It("forwards event messages to the handler", func() {
			called := false
			upstream.HandleEventFunc = func(
				_ context.Context,
				_ dogma.ProcessRoot,
				_ dogma.ProcessEventScope,
				m dogma.Event,
			) error {
				called = true
				Expect(m).To(Equal(EventE1))
				return nil
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(called).To(BeTrue())
		})

		It("returns an error if the handler returns an error", func() {
			upstream.HandleEventFunc = func(
				_ context.Context,
				_ dogma.ProcessRoot,
				_ dogma.ProcessEventScope,
				m dogma.Event,
			) error {
				return errors.New("<error>")
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).To(MatchError("<error>"))
		})

		It("makes the instance ID available via the scope ", func() {
			upstream.HandleEventFunc = func(
				_ context.Context,
				_ dogma.ProcessRoot,
				s dogma.ProcessEventScope,
				_ dogma.Event,
			) error {
				Expect(s.InstanceID()).To(Equal("<instance>"))
				return nil
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("makes the recorded-at time available via the scope", func() {
			upstream.HandleEventFunc = func(
				_ context.Context,
				_ dogma.ProcessRoot,
				s dogma.ProcessEventScope,
				_ dogma.Event,
			) error {
				Expect(s.RecordedAt()).To(BeTemporally("==", cause.CreatedAt))
				return nil
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns an error if the instance can not be loaded", func() {
			dataStore.LoadProcessInstanceFunc = func(
				context.Context,
				string,
				string,
			) (persistence.ProcessInstance, error) {
				return persistence.ProcessInstance{}, errors.New("<error>")
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).To(MatchError("<error>"))
		})

		It("returns an error if the instance can not be routed", func() {
			upstream.RouteEventToInstanceFunc = func(
				context.Context,
				dogma.Event,
			) (string, bool, error) {
				return "", false, errors.New("<error>")
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).To(MatchError("<error>"))
		})

		It("panics if the handler routes the message to an empty instance ID", func() {
			upstream.RouteEventToInstanceFunc = func(
				context.Context,
				dogma.Event,
			) (string, bool, error) {
				return "", true, nil
			}

			Expect(func() {
				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			}).To(PanicWith("*stubs.ProcessMessageHandlerStub.RouteEventToInstance() returned an empty instance ID while routing a stubs.EventStub[github.com/dogmatiq/enginekit/enginetest/stubs.TypeE] event"))
		})

		It("skips the message if the handler does not route it to an instance", func() {
			upstream.RouteEventToInstanceFunc = func(
				context.Context,
				dogma.Event,
			) (string, bool, error) {
				return "", false, nil
			}

			upstream.HandleEventFunc = func(
				context.Context,
				dogma.ProcessRoot,
				dogma.ProcessEventScope,
				dogma.Event,
			) error {
				Fail("unexpected call")
				return nil
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("panics if the handler returns a nil root", func() {
			upstream.NewFunc = func() dogma.ProcessRoot {
				return nil
			}

			Expect(func() {
				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			}).To(PanicWith("*stubs.ProcessMessageHandlerStub.New() returned nil"))
		})

		It("saves the process instance", func() {
			upstream.HandleEventFunc = func(
				_ context.Context,
				r dogma.ProcessRoot,
				s dogma.ProcessEventScope,
				_ dogma.Event,
			) error {
				r.(*ProcessRootStub).Value = "<value>"

				return nil
			}

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(work.Operations).To(EqualX(
				[]persistence.Operation{
					persistence.SaveProcessInstance{
						Instance: persistence.ProcessInstance{
							HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
							InstanceID: "<instance>",
							Packet: marshalkit.Packet{
								MediaType: "application/json; type=ProcessRootStub",
								Data:      []byte(`{"value":"\u003cvalue\u003e"}`),
							},
						},
					},
				},
			))
		})

		It("returns an error if the process instance can not be marshaled", func() {
			adaptor.Marshaler = &codec.Marshaler{} // an empty marshaler cannot marshal anything

			err := adaptor.HandleMessage(ctx, work, cause)
			Expect(err).To(MatchError("no codecs support the '*stubs.ProcessRootStub' type"))
		})

		When("a command is executed", func() {
			It("saves the command", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					s.ExecuteCommand(CommandC1)
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(work.Commands).To(EqualX(
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
								Description:       "command(stubs.TypeC:C1, valid)",
								PortableName:      "CommandStub[TypeC]",
								MediaType:         `application/json; type="CommandStub[TypeC]"`,
								Data:              []byte(`{"content":"C1"}`),
							},
							Message:   CommandC1,
							CreatedAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
						},
					},
				))
			})

			It("logs about the command", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					s.ExecuteCommand(CommandC1)
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    CommandStub[TypeC] ● command(stubs.TypeC:C1, valid)",
					},
				))
			})

			It("reverts a prior call to End()", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					r dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					r.(*ProcessRootStub).Value = "<value>"
					s.End()
					s.ExecuteCommand(CommandC1)
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(work.Operations).To(EqualX(
					[]persistence.Operation{
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
								InstanceID: "<instance>",
								Packet: marshalkit.Packet{
									MediaType: "application/json; type=ProcessRootStub",
									Data:      []byte(`{"value":"\u003cvalue\u003e"}`),
								},
							},
						},
					},
				))
			})
		})

		When("a timeout is scheduled", func() {
			It("saves the timeout", func() {
				scheduledFor := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

				upstream.HandleEventFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					s.ScheduleTimeout(TimeoutT1, scheduledFor)
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(work.Timeouts).To(EqualX(
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
								ScheduledFor:      "2020-01-01T00:00:00Z",
								Description:       "timeout(stubs.TypeT:T1, valid)",
								PortableName:      "TimeoutStub[TypeT]",
								MediaType:         `application/json; type="TimeoutStub[TypeT]"`,
								Data:              []byte(`{"content":"T1"}`),
							},
							Message:      TimeoutT1,
							CreatedAt:    time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
							ScheduledFor: scheduledFor,
						},
					},
				))
			})

			It("logs about the timeout", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					s.ScheduleTimeout(TimeoutT1, time.Now())
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= 0  ∵ <consume>  ⋲ <correlation>  ▲    TimeoutStub[TypeT] ● timeout(stubs.TypeT:T1, valid)",
					},
				))
			})

			It("reverts a prior call to End()", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					r dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					r.(*ProcessRootStub).Value = "<value>"
					s.End()
					s.ScheduleTimeout(TimeoutT1, time.Now())
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				Expect(work.Operations).To(EqualX(
					[]persistence.Operation{
						persistence.SaveProcessInstance{
							Instance: persistence.ProcessInstance{
								HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
								InstanceID: "<instance>",
								Packet: marshalkit.Packet{
									MediaType: "application/json; type=ProcessRootStub",
									Data:      []byte(`{"value":"\u003cvalue\u003e"}`),
								},
							},
						},
					},
				))
			})
		})

		When("a message is logged via the scope", func() {
			BeforeEach(func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					s.Log("format %s", "<value>")
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			It("logs using the standard format", func() {
				Expect(logger.Messages()).To(ContainElement(
					logging.BufferedLogMessage{
						Message: "= <consume>  ∵ <cause>  ⋲ <correlation>  ▼    EventStub[TypeE] ● format <value>",
					},
				))
			})
		})

		It("returns an error if the deadline is exceeded while acquiring the cache record", func() {
			_, err := adaptor.Cache.Acquire(ctx, &UnitOfWorkStub{}, "<instance>")
			Expect(err).ShouldNot(HaveOccurred())

			ctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
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

		When("the instance does not already exist", func() {
			It("does not panic if the instance is ended", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					s.End()
					return nil
				}

				Expect(func() {
					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
				}).NotTo(Panic())
			})

			It("does not action timeout messages", func() {
				cause = NewParcel("<consume>", TimeoutT1, time.Now(), time.Now())

				upstream.HandleTimeoutFunc = func(
					_ context.Context,
					_ dogma.ProcessRoot,
					_ dogma.ProcessTimeoutScope,
					m dogma.Timeout,
				) error {
					Fail("unexpected call")
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})
		})

		When("the instance already exists", func() {
			BeforeEach(func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					r dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					r.(*ProcessRootStub).Value = "<value>"

					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())

				work.Succeed(handler.Result{})
				work = &UnitOfWorkStub{}

				upstream.HandleEventFunc = nil
			})

			It("provides a root with the correct state", func() {
				upstream.HandleEventFunc = func(
					_ context.Context,
					r dogma.ProcessRoot,
					s dogma.ProcessEventScope,
					_ dogma.Event,
				) error {
					Expect(r.(*ProcessRootStub).Value).To(Equal("<value>"))
					return nil
				}

				err := adaptor.HandleMessage(ctx, work, cause)
				Expect(err).ShouldNot(HaveOccurred())
			})

			When("the instance is ended", func() {
				It("removes the process instance", func() {
					upstream.HandleEventFunc = func(
						_ context.Context,
						_ dogma.ProcessRoot,
						s dogma.ProcessEventScope,
						_ dogma.Event,
					) error {
						s.End()
						return nil
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(work.Operations).To(EqualX(
						[]persistence.Operation{
							persistence.RemoveProcessInstance{
								Instance: persistence.ProcessInstance{
									HandlerKey: "2ae0b937-e806-4e70-9b23-f36298f68973",
									InstanceID: "<instance>",
									Revision:   1,
									Packet: marshalkit.Packet{
										MediaType: "application/json; type=ProcessRootStub",
										Data:      []byte(`{"value":"\u003cvalue\u003e"}`),
									},
								},
							},
						},
					))
				})
			})

			When("a timeout message is being handled", func() {
				BeforeEach(func() {
					cause = NewParcel("<consume>", TimeoutT1, time.Now(), time.Now())

					upstream.RouteEventToInstanceFunc = func(
						context.Context,
						dogma.Event,
					) (string, bool, error) {
						Fail("unexpected call")
						return "", false, nil
					}
				})

				It("forwards timeout messages to the handler", func() {
					called := false
					upstream.HandleTimeoutFunc = func(
						_ context.Context,
						_ dogma.ProcessRoot,
						_ dogma.ProcessTimeoutScope,
						m dogma.Timeout,
					) error {
						called = true
						Expect(m).To(Equal(TimeoutT1))
						return nil
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(called).To(BeTrue())
				})

				It("returns an error if the handler returns an error", func() {
					upstream.HandleTimeoutFunc = func(
						_ context.Context,
						_ dogma.ProcessRoot,
						_ dogma.ProcessTimeoutScope,
						m dogma.Timeout,
					) error {
						return errors.New("<error>")
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).To(MatchError("<error>"))
				})

				It("makes the instance ID available via the scope", func() {
					upstream.HandleTimeoutFunc = func(
						_ context.Context,
						_ dogma.ProcessRoot,
						s dogma.ProcessTimeoutScope,
						_ dogma.Timeout,
					) error {
						Expect(s.InstanceID()).To(Equal("<instance>"))
						return nil
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
				})

				It("makes the scheduled-for time available via the scope", func() {
					upstream.HandleTimeoutFunc = func(
						_ context.Context,
						_ dogma.ProcessRoot,
						s dogma.ProcessTimeoutScope,
						_ dogma.Timeout,
					) error {
						Expect(s.ScheduledFor()).To(BeTemporally("==", cause.ScheduledFor))
						return nil
					}

					err := adaptor.HandleMessage(ctx, work, cause)
					Expect(err).ShouldNot(HaveOccurred())
				})
			})
		})
	})
})
