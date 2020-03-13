package infix

import (
	"github.com/dogmatiq/infix/persistence/provider/boltdb"
	"github.com/dogmatiq/marshalkit/codec"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func WithPersistenceProvider()", func() {
	It("sets the persistence provider", func() {
		p := &boltdb.Provider{
			File: "<filename>",
		}

		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithPersistenceProvider(p),
		})

		Expect(opts.PersistenceProvider).To(Equal(p))
	})

	It("uses the default if the provider is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithPersistenceProvider(nil),
		})

		Expect(opts.PersistenceProvider).To(Equal(DefaultPersistenceProvider))
	})
})

var _ = Describe("func WithMarshaler()", func() {
	It("sets the marshaler", func() {
		m := &codec.Marshaler{}

		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMarshaler(m),
		})

		Expect(opts.Marshaler).To(BeIdenticalTo(m))
	})

	It("constructs a default if the marshaler is nil", func() {
		opts := resolveOptions([]EngineOption{
			WithApplication(TestApplication),
			WithMarshaler(nil),
		})

		Expect(opts.Marshaler).To(Equal(
			NewDefaultMarshaler(opts.AppConfigs),
		))
	})
})
