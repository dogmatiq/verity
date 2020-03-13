package infix

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
)

var _ = Describe("func WithListenAddress()", func() {
	It("sets the listener address", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithListenAddress("localhost:1234"),
		})

		Expect(opts.ListenAddress).To(Equal("localhost:1234"))
	})

	It("uses the default if the address is empty", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
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

var _ = Describe("func WithServerOptions()", func() {
	It("appends to the options", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithServerOptions(grpc.ConnectionTimeout(0)),
			WithServerOptions(grpc.ConnectionTimeout(0)),
		})

		Expect(opts.ServerOptions).To(HaveLen(2))
	})
})
