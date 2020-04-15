package envelopespec_test

import (
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	. "github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("func CheckWellFormed()", func() {
	var env *Envelope

	BeforeEach(func() {
		env = NewEnvelope(
			"<id>",
			MessageA1,
			time.Now(),
			time.Now(),
		)
	})

	It("does not return an error if the envelope is well-formed", func() {
		err := CheckWellFormed(env)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("returns an error if the message ID is empty", func() {
		env.MetaData.MessageId = ""

		err := CheckWellFormed(env)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the causation ID is empty", func() {
		env.MetaData.CausationId = ""

		err := CheckWellFormed(env)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the correlation ID is empty", func() {
		env.MetaData.CorrelationId = ""

		err := CheckWellFormed(env)
		Expect(err).Should(HaveOccurred())
	})

	It("returns an error if the source app name is empty", func() {
		env.MetaData.Source.Application.Name = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("application identity is invalid: identity name must not be empty"))
	})

	It("returns an error if the source app key is empty", func() {
		env.MetaData.Source.Application.Key = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("application identity is invalid: identity key must not be empty"))
	})

	It("returns an error if the source handler name is empty", func() {
		env.MetaData.Source.Handler.Name = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("handler identity is invalid: identity name must not be empty"))
	})

	It("returns an error if the source handler key is empty", func() {
		env.MetaData.Source.Handler.Key = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("handler identity is invalid: identity key must not be empty"))
	})

	It("returns an error if the source handler is empty but the instance ID is set", func() {
		env.MetaData.Source.Handler = nil

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("source instance ID must not be specified without providing a handler identity"))
	})

	When("there is no source handler", func() {
		BeforeEach(func() {
			env.MetaData.Source.Handler = nil
			env.MetaData.Source.InstanceId = ""
		})

		It("returns an error if the message is a timeout", func() {
			err := CheckWellFormed(env)
			Expect(err).To(MatchError("scheduled-for time must not be specified without a providing source handler and instance ID"))
		})

		It("does not return an error if the message is not a timeout", func() {
			env.MetaData.ScheduledFor = ""

			err := CheckWellFormed(env)
			Expect(err).ShouldNot(HaveOccurred())
		})
	})

	It("does not return an error if the message description is empty", func() {
		env.MetaData.Description = ""

		err := CheckWellFormed(env)
		Expect(err).ShouldNot(HaveOccurred())
	})

	It("returns an error if the created-at timestamp is empty", func() {
		env.MetaData.CreatedAt = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("created-at time must not be empty"))
	})

	It("returns an error if the portable name is empty", func() {
		env.PortableName = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("portable name must not be empty"))
	})

	It("returns an error if the media-type is empty", func() {
		env.MediaType = ""

		err := CheckWellFormed(env)
		Expect(err).To(MatchError("media-type must not be empty"))
	})

	It("does not return an error if the message data is empty", func() {
		env.Data = nil

		err := CheckWellFormed(env)
		Expect(err).ShouldNot(HaveOccurred())
	})
})

var _ = Describe("func MustBeWellFormed()", func() {
	It("panics if the envelope is not well-formed", func() {
		env := NewEnvelope(
			"<id>",
			MessageA1,
			time.Now(),
			time.Now(),
		)
		env.MetaData = nil

		Expect(func() {
			MustBeWellFormed(env)
		}).To(Panic())
	})
})
