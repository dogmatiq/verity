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

var _ = Describe("func WithDiscoverer()", func() {
	discoverer := func(ctx context.Context, obs discovery.TargetObserver) error {
		return errors.New("<error>")
	}

	It("sets the discoverer", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithDiscoverer(discoverer),
		})

		err := opts.Discoverer(context.Background(), nil)
		Expect(err).To(MatchError("<error>"))
	})

	It("does not construct a default if the discoverer is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithDiscoverer(nil),
		})

		Expect(opts.Discoverer).To(BeNil())
	})
})

var _ = Describe("func WithDialer()", func() {
	discoverer := func(ctx context.Context, obs discovery.TargetObserver) error {
		panic("not implemented")
	}

	dialer := func(ctx context.Context, t *discovery.Target) (*grpc.ClientConn, error) {
		return nil, errors.New("<error>")
	}

	It("sets the dialer", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithDiscoverer(discoverer),
			WithDialer(dialer),
		})

		_, err := opts.Dialer(context.Background(), nil)
		Expect(err).To(MatchError("<error>"))
	})

	It("constructs a default if the dialer is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithDiscoverer(discoverer),
			WithDiscoverer(nil),
		})

		Expect(opts.Dialer).NotTo(BeNil())
	})

	It("causes a panic if WithDiscoverer() is not used", func() {
		Expect(func() {
			resolveOptions([]EngineOption{
				WithApplication(TestApplication),
				WithDialer(dialer),
			})
		}).To(Panic())
	})
})

var _ = Describe("func WithDialerBackoffStrategy()", func() {
	It("sets the backoff strategy", func() {
		p := backoff.Constant(10 * time.Second)

		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithDialerBackoffStrategy(p),
		})

		Expect(opts.DialerBackoffStrategy(nil, 1)).To(Equal(10 * time.Second))
	})

	It("uses the default if the strategy is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithDialerBackoffStrategy(nil),
		})

		Expect(opts.DialerBackoffStrategy).ToNot(BeNil())
	})
})
