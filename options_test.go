package infix

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/dodeca/logging"
	"github.com/dogmatiq/dogma"
	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/linger/backoff"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("type EngineOption", func() {
	var app dogma.Application

	BeforeSuite(func() {
		app = &Application{
			ConfigureFunc: func(c dogma.ApplicationConfigurer) {
				c.Identity("<app-name>", "<app-key>")
			},
		}
	})

	Describe("func WithApplication()", func() {
		It("adds the application", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
			})

			Expect(opts.AppConfigs).To(ConsistOf(
				configkit.FromApplication(app),
			))
		})

		It("panics if the none are provided", func() {
			Expect(func() {
				resolveOptions(nil)
			}).To(Panic())
		})
	})

	Describe("func WithListenAddress()", func() {
		It("sets the listener address", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithListenAddress("localhost:1234"),
			})

			Expect(opts.ListenAddress).To(Equal("localhost:1234"))
		})

		It("uses the default if the address is empty", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithListenAddress(""),
			})

			Expect(opts.ListenAddress).To(Equal(DefaultListenAddress))
		})

		It("panics if the address is invalid", func() {
			Expect(func() {
				WithListenAddress("missing-port")
			}).To(Panic())
		})

		It("panics if the post is an unknown service name", func() {
			Expect(func() {
				WithListenAddress("host:xxx")
			}).To(Panic())
		})
	})

	Describe("func WithMessageTimeout()", func() {
		It("sets the message timeout", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithMessageTimeout(10 * time.Minute),
			})

			Expect(opts.MessageTimeout).To(Equal(10 * time.Minute))
		})

		It("uses the default if the duration is zero", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithMessageTimeout(0),
			})

			Expect(opts.MessageTimeout).To(Equal(DefaultMessageTimeout))
		})

		It("panics if the duration is less than zero", func() {
			Expect(func() {
				WithMessageTimeout(-1)
			}).To(Panic())
		})
	})

	Describe("func WithMessageBackoffStrategy()", func() {
		It("sets the backoff strategy", func() {
			p := backoff.Constant(10 * time.Second)

			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithMessageBackoffStrategy(p),
			})

			Expect(opts.MessageBackoffStrategy(nil, 1)).To(Equal(10 * time.Second))
		})

		It("uses the default if the strategy is nil", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithMessageBackoffStrategy(nil),
			})

			Expect(opts.MessageBackoffStrategy).ToNot(BeNil())
		})
	})

	Describe("func WithDiscoverer()", func() {
		discoverer := func(ctx context.Context, obs discovery.TargetObserver) error {
			return errors.New("<error>")
		}

		It("sets the discoverer", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithDiscoverer(discoverer),
			})

			err := opts.Discoverer(context.Background(), nil)
			Expect(err).To(MatchError("<error>"))
		})

		It("does not construct a default if the discoverer is nil", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithDiscoverer(nil),
			})

			Expect(opts.Discoverer).To(BeNil())
		})
	})

	Describe("func WithDialer()", func() {
		discoverer := func(ctx context.Context, obs discovery.TargetObserver) error {
			panic("not implemented")
		}

		dialer := func(ctx context.Context, t *discovery.Target) (*grpc.ClientConn, error) {
			return nil, errors.New("<error>")
		}

		It("sets the dialer", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithDiscoverer(discoverer),
				WithDialer(dialer),
			})

			_, err := opts.Dialer(context.Background(), nil)
			Expect(err).To(MatchError("<error>"))
		})

		It("constructs a default if the dialer is nil", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithDiscoverer(discoverer),
				WithDiscoverer(nil),
			})

			Expect(opts.Dialer).NotTo(BeNil())
		})

		It("causes a panic if WithDiscoverer() is not used", func() {
			Expect(func() {
				resolveOptions([]EngineOption{
					WithApplication(app),
					WithDialer(dialer),
				})
			}).To(Panic())
		})
	})

	Describe("func WithDialerBackoffStrategy()", func() {
		It("sets the backoff strategy", func() {
			p := backoff.Constant(10 * time.Second)

			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithDialerBackoffStrategy(p),
			})

			Expect(opts.DialerBackoffStrategy(nil, 1)).To(Equal(10 * time.Second))
		})

		It("uses the default if the strategy is nil", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithDialerBackoffStrategy(nil),
			})

			Expect(opts.DialerBackoffStrategy).ToNot(BeNil())
		})
	})

	Describe("func WithServerOptions()", func() {
		It("appends to the options", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithServerOptions(grpc.ConnectionTimeout(0)),
				WithServerOptions(grpc.ConnectionTimeout(0)),
			})

			Expect(opts.ServerOptions).To(HaveLen(2))
		})
	})

	Describe("func WithMarshaler()", func() {
		It("sets the marshaler", func() {
			m := &codec.Marshaler{}

			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithMarshaler(m),
			})

			Expect(opts.Marshaler).To(BeIdenticalTo(m))
		})

		It("constructs a default if the marshaler is nil", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithMarshaler(nil),
			})

			Expect(opts.Marshaler).To(Equal(
				NewDefaultMarshaler(opts.AppConfigs),
			))
		})
	})

	Describe("func WithLogger()", func() {
		It("sets the logger", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithLogger(logging.DebugLogger),
			})

			Expect(opts.Logger).To(BeIdenticalTo(logging.DebugLogger))
		})

		It("uses the default if the logger is nil", func() {
			opts := resolveOptions([]EngineOption{
				WithApplication(app),
				WithLogger(nil),
			})

			Expect(opts.Logger).To(Equal(DefaultLogger))
		})
	})
})
