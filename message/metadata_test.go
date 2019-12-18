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
			MessageID:     "<id>",
			CausationID:   "<id>",
			CorrelationID: "<id>",
			Source: Source{
				Application: configkit.MustNewIdentity("<app-name>", "<app-key>"),
				Handler:     configkit.MustNewIdentity("<handler-name>", "<handler-key>"),
				InstanceID:  "<instance>",
			},
			CreatedAt:    time.Now(),
			ScheduledFor: time.Now(),
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
			md.Source.Application = configkit.Identity{}

			err := md.Validate()
			Expect(err).To(MatchError("source app name must not be empty"))
		})

		It("returns an error if the source handler is empty but the instance ID is set", func() {
			md.Source.Handler = configkit.Identity{}

			err := md.Validate()
			Expect(err).To(MatchError("source handler name must not be empty when source instance ID is present"))
		})

		It("does not return an error of both the source handler and instance ID are empty", func() {
			md.Source.Handler = configkit.Identity{}
			md.Source.InstanceID = ""

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
