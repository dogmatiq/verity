package parcel_test

import (
	"time"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	. "github.com/dogmatiq/verity/fixtures"
	. "github.com/dogmatiq/verity/parcel"
	. "github.com/jmalloc/gomegax"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("type Parcel", func() {
	var (
		createdAt, scheduledFor time.Time
		env                     *envelopepb.Envelope
	)

	BeforeEach(func() {
		createdAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
		scheduledFor = time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC)

		env = NewEnvelope(
			"<id>",
			CommandA1,
			createdAt,
			scheduledFor,
		)
	})

	Describe("func ID()", func() {
		It("returns the ID from the envelope", func() {
			p := Parcel{
				Envelope: env,
			}

			Expect(p.ID()).To(Equal("<id>"))
		})
	})

	Describe("func FromEnvelope()", func() {
		It("returns a parcel containing the given envelope", func() {
			p, err := FromEnvelope(env)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(p).To(EqualX(
				Parcel{
					Envelope:     env,
					Message:      CommandA1,
					CreatedAt:    createdAt,
					ScheduledFor: scheduledFor,
				},
			))
		})

		It("returns an error if the envelope is not well-formed", func() {
			env.MessageId = nil

			_, err := FromEnvelope(env)
			Expect(err).Should(HaveOccurred())
		})

		It("returns an error if the message can not be unmarshaled", func() {
			env.Data = []byte("<malformed>")

			_, err := FromEnvelope(env)
			Expect(err).Should(HaveOccurred())
		})
	})
})
