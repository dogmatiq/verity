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

			m, ok := pq.PopFront()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message0))
		})
	})

	Describe("func PopFront()", func() {
		It("pops messages in order of priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.PopFront()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message0))
		})

		It("returns false if the queue is empty", func() {
			pq.Push(message0)

			_, ok := pq.PopFront()
			Expect(ok).To(BeTrue())

			_, ok = pq.PopFront()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PopBack()", func() {
		It("pops messages in reverse order of priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.PopBack()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message2))
		})

		It("returns false if the queue is empty", func() {
			pq.Push(message0)

			_, ok := pq.PopBack()
			Expect(ok).To(BeTrue())

			_, ok = pq.PopBack()
			Expect(ok).To(BeFalse())
		})
	})
})
