package processor

import (
	"time"

	"github.com/dogmatiq/infix/persistence/subsystem/queue"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("type pqueue()", func() {
	var (
		pq *pqueue

		message0 = &queue.Message{
			Revision:      0,
			NextAttemptAt: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		}

		message1 = &queue.Message{
			Revision:      1,
			NextAttemptAt: time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC),
		}

		message2 = &queue.Message{
			Revision:      2,
			NextAttemptAt: time.Date(2002, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	)

	BeforeEach(func() {
		pq = &pqueue{}
	})

	Describe("func Push()", func() {
		It("adds a message to the queue", func() {
			pq.Push(message0)
			Expect(pq.Len()).To(Equal(1))
		})

		It("returns true if the new message has the highest priority", func() {
			pq.Push(message1)

			Expect(pq.Push(message2)).To(BeFalse())
			Expect(pq.Push(message0)).To(BeTrue())
		})
	})

	Describe("func PeekFront()", func() {
		It("returns the message with the highest priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.PeekFront()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message0))

			Expect(pq.Len()).To(
				Equal(3),
				"PeekFront() modified the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := pq.PeekFront()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PopFront()", func() {
		It("removes and returns the message with the highest priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.PopFront()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message0))

			Expect(pq.Len()).To(
				Equal(2),
				"PopFront() did not modify the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := pq.PopFront()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PeekBack()", func() {
		It("returns the message with the lowest priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.PeekBack()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message2))

			Expect(pq.Len()).To(
				Equal(3),
				"PeekBack() modified the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := pq.PeekBack()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PopBack()", func() {
		It("removes and returns the message with the lowest priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.PopBack()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message2))

			Expect(pq.Len()).To(
				Equal(2),
				"PopBack() did not modify the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := pq.PopBack()
			Expect(ok).To(BeFalse())
		})
	})
})
