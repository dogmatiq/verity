package infix

import (
	"context"
	"errors"
	"time"

	"github.com/dogmatiq/configkit/api/discovery"
	"github.com/dogmatiq/linger/backoff"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("func WithNetworking()", func() {
	It("sets the network options", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
			WithNetworking(),
		)

		Expect(opts.Network).ToNot(BeNil())
	})

	It("does not construct a default if the option is omitted", func() {
		opts := resolveEngineOptions(
			WithApplication(TestApplication),
		)

		Expect(opts.Network).To(BeNil())
	})
})

var _ = Describe("func WithListenAddress()", func() {
	It("sets the listener address", func() {
		opts := resolveNetworkOptions(
			WithListenAddress("localhost:1234"),
		)

		Expect(opts.ListenAddress).To(Equal("localhost:1234"))
	})

	It("uses the default if the address is empty", func() {
		opts := resolveNetworkOptions(
			WithListenAddress(""),
		)

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

var _ = Describe("func WithServerOptions()", func() {
	It("appends to the options", func() {
		opts := resolveNetworkOptions(
			WithServerOptions(grpc.ConnectionTimeout(0)),
			WithServerOptions(grpc.ConnectionTimeout(0)),
		)

		Expect(opts.ServerOptions).To(HaveLen(2))
	})
})

var _ = Describe("func WithDialer()", func() {
	It("sets the dialer", func() {
		dialer := func(ctx context.Context, t *discovery.Target) (*grpc.ClientConn, error) {
			return nil, errors.New("<error>")
		}

		opts := resolveNetworkOptions(
			WithDialer(dialer),
		)

		conn, err := opts.Dialer(context.Background(), nil)
		if conn != nil {
			conn.Close()
		}
		Expect(err).To(MatchError("<error>"))
	})

	It("uses the default if the dialer is nil", func() {
		opts := resolveNetworkOptions(
			WithDialer(nil),
		)

		Expect(opts.Dialer).ToNot(BeNil())
	})
})

var _ = Describe("func WithDialerBackoff()", func() {
	It("sets the backoff strategy", func() {
		p := backoff.Constant(10 * time.Second)

		opts := resolveNetworkOptions(
			WithDialerBackoff(p),
		)

		Expect(opts.DialerBackoff(nil, 1)).To(Equal(10 * time.Second))
	})

	It("uses the default if the strategy is nil", func() {
		opts := resolveNetworkOptions(
			WithDialerBackoff(nil),
		)

		Expect(opts.DialerBackoff).ToNot(BeNil())
	})
})

var _ = Describe("func WithDiscoverer()", func() {
	It("sets the discoverer", func() {
		discoverer := func(ctx context.Context, obs discovery.TargetObserver) error {
			return errors.New("<error>")
		}

		opts := resolveNetworkOptions(
			WithDiscoverer(discoverer),
		)

		err := opts.Discoverer(context.Background(), nil)
		Expect(err).To(MatchError("<error>"))
	})

	It("uses the default if the discoverer is nil", func() {
		opts := resolveNetworkOptions(
			WithDiscoverer(nil),
		)

		err := opts.Discoverer(context.Background(), nil)
		// TODO: https://github.com/dogmatiq/configkit/issues/58
		Expect(err).To(MatchError("no API discovery configured, see infix.WithDiscoverer()"))
	})
})
