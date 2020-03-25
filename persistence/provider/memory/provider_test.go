package memory_test

import (
	"context"

	"github.com/dogmatiq/infix/persistence/provider/internal/providertest"
	. "github.com/dogmatiq/infix/persistence/provider/memory"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("type Provider", func() {
	providertest.Declare(
		func(ctx context.Context, in providertest.In) providertest.Out {
			return providertest.Out{
				Provider: &Provider{},
			}
		},
		nil,
	)
})
