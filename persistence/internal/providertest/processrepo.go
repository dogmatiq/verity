package providertest

import (
	"github.com/dogmatiq/infix/persistence"
	"github.com/onsi/ginkgo"
	"github.com/onsi/gomega"
)

// declareProcessRepositoryTests declares a functional test-suite for a
// specific persistence.ProcessRepository implementation.
func declareProcessRepositoryTests(tc *TestContext) {
	ginkgo.Describe("type persistence.ProcessRepository", func() {
		var (
			dataStore persistence.DataStore
			tearDown  func()
		)

		ginkgo.BeforeEach(func() {
			dataStore, tearDown = tc.SetupDataStore()
		})

		ginkgo.AfterEach(func() {
			tearDown()
		})

		ginkgo.Describe("func LoadProcessInstance()", func() {
			ginkgo.It("returns an instance with default values if the instance does not exist", func() {
				inst := loadProcessInstance(tc.Context, dataStore, "<handler-key>", "<instance>")
				gomega.Expect(inst).To(gomega.Equal(
					persistence.ProcessInstance{
						HandlerKey: "<handler-key>",
						InstanceID: "<instance>",
					},
				))
			})
		})
	})
}
