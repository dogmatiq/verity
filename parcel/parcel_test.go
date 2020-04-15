package parcel_test

import (
	"time"

	. "github.com/dogmatiq/dogma/fixtures"
	"github.com/dogmatiq/infix/draftspecs/envelopespec"
	. "github.com/dogmatiq/infix/fixtures"
	. "github.com/dogmatiq/infix/internal/x/gomegax"
	. "github.com/dogmatiq/infix/parcel"
	. "github.com/dogmatiq/marshalkit/fixtures"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Parcel", func() {
	var (
		createdAt, scheduledFor time.Time
		env                     *envelopespec.Envelope
	)

	BeforeEach(func() {
		createdAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		scheduledFor = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

		env = NewEnvelope(
			"<id>",
			MessageA1,
			createdAt,
			scheduledFor,
		)
	})

	Describe("func FromEnvelope()", func() {
		It("returns a parcel containing the given envelope", func() {
			p, err := FromEnvelope(Marshaler, env)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(p).To(EqualX(
				&Parcel{
					Envelope:     env,
					Message:      MessageA1,
					CreatedAt:    createdAt,
					ScheduledFor: scheduledFor,
				},
			))
		})

		It("returns an error if the envelope is not well-formed", func() {
			env.MetaData.MessageId = ""

			_, err := FromEnvelope(Marshaler, env)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the message can not be unmarshaled", func() {
			env.Data = []byte("<malformed>")

			_, err := FromEnvelope(Marshaler, env)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the created-at time can not be unmarshaled", func() {
			env.MetaData.CreatedAt = "<malformed>"

			_, err := FromEnvelope(Marshaler, env)
			Expect(err).Should(HaveOccurred())
		})
		It("returns an error if the scheduled-for time can not be unmarshaled", func() {
			env.MetaData.ScheduledFor = "<malformed>"

			_, err := FromEnvelope(Marshaler, env)
			Expect(err).Should(HaveOccurred())
		})
	})
})
