package message_test

import (
	"time"

	"github.com/dogmatiq/configkit"
	. "github.com/dogmatiq/infix/message"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type MetaData", func() {
	var md *MetaData

	BeforeEach(func() {
		md = &MetaData{
			MessageID:        "<id>",
			CausationID:      "<id>",
			CorrelationID:    "<id>",
			SourceApp:        configkit.MustNewIdentity("<app-name>", "<app-key>"),
			SourceHandler:    configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
			SourceInstanceID: "<instance>",
			CreatedAt:        time.Now(),
			ScheduledFor:     time.Now(),
		}
	})

	Describe("func Validate()", func() {
		It("does not return an error if the data is fully populated", func() {
			err := md.Validate()
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns an error if the message ID is invalid", func() {
			md.MessageID = ""

			err := md.Validate()
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the causation ID is invalid", func() {
			md.CausationID = ""

			err := md.Validate()
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the correlation ID is invalid", func() {
			md.CorrelationID = ""

			err := md.Validate()
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the source app is empty", func() {
			md.SourceApp = configkit.Identity{}

			err := md.Validate()
			Expect(err).To(MatchError("source app name must not be empty"))
		})

		It("returns an error if the source handler is empty but the instance ID is set", func() {
			md.SourceHandler = configkit.Identity{}

			err := md.Validate()
			Expect(err).To(MatchError("source handler name must not be empty when source instance ID is present"))
		})

		It("does not return an error of both the source handler and instance ID are empty", func() {
			md.SourceHandler = configkit.Identity{}
			md.SourceInstanceID = ""

			err := md.Validate()
			Expect(err).ShouldNot(HaveOccurred())
		})

		It("returns an error if the created-at timestamp is zero", func() {
			md.CreatedAt = time.Time{}

			err := md.Validate()
			Expect(err).To(MatchError("created-at time must not be zero"))
		})
	})
})
