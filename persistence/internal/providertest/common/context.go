package common

import (
	"context"
	"time"

	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"github.com/onsi/gomega"
)

// In is a container for values provided by the test suite to the
// provider-specific initialization code.
type In struct {
	// Marshaler marshals and unmarshals the test message types, aggregate roots
	// and process roots.
	Marshaler marshalkit.Marshaler
}

// Out is a container for values that are provided by the provider-specific
// initialization code to the test suite.
type Out struct {
	// NewProvider is a function that creates a new provider.
	NewProvider func() (p persistence.Provider, close func())

	// IsShared returns true if multiple instances of the same provider access
	// the same data.
	IsShared bool

	// TestTimeout is the maximum duration allowed for each test.
	TestTimeout time.Duration
}

// DefaultTestTimeout is the default test timeout.
const DefaultTestTimeout = 3 * time.Second

// TestContext encapsulates the shared test context passed to the tests for each
// provider sub-system.
type TestContext struct {
	Context context.Context
	In      In
	Out     Out
}

// SetupDataStore sets up a new data-store for the "<app-key>" application.
func (tc *TestContext) SetupDataStore() (persistence.DataStore, func()) {
	p, close := tc.Out.NewProvider()

	ds, err := p.Open(tc.Context, "<app-key>")
	if err != nil {
		close()
		gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
	}

	return ds, func() {
		ds.Close()
		close()
	}
}
