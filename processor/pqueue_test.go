package processor

import (
	"math/rand"
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

		message3 = &queue.Message{
			Revision:      3,
			NextAttemptAt: time.Date(2003, 1, 1, 0, 0, 0, 0, time.UTC),
		}

		message4 = &queue.Message{
			Revision:      4,
			NextAttemptAt: time.Date(2004, 1, 1, 0, 0, 0, 0, time.UTC),
		}
	)

	BeforeEach(func() {
		pq = &pqueue{
			limit: 100,
		}
	})

	Describe("func Push()", func() {
		It("adds a message to the queue", func() {
			pq.Push(message0)

			m, ok := pq.Pop()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message0))
		})

		It("never adds messages if the size limit is zero", func() {
			pq.limit = 0

			pq.Push(message0)

			_, ok := pq.Pop()
			Expect(ok).To(BeFalse())
		})

		It("drops the lowest priority message from the queue if the size limit is exceeded", func() {
			pq.limit = 4

			messages := []*queue.Message{
				message0,
				message1,
				message2,
				message3,
				message4,
			}

			shuffled := append([]*queue.Message{}, messages...)

			rand.Shuffle(
				len(shuffled),
				func(i, j int) {
					shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
				},
			)

			for _, m := range shuffled {
				pq.Push(m)
			}

			expected := messages[:pq.limit]
			var queued []*queue.Message

			for {
				m, ok := pq.Pop()
				if !ok {
					break
				}

				queued = append(queued, m)
			}

			Expect(queued).To(Equal(expected))
		})
	})

	Describe("func Pop()", func() {
		It("pops messages in order of priority", func() {
			pq.Push(message2)
			pq.Push(message0)
			pq.Push(message1)

			m, ok := pq.Pop()
			Expect(ok).To(BeTrue())
			Expect(m).To(BeIdenticalTo(message0))
		})

		It("returns false if the queue is empty", func() {
			pq.Push(message0)

			_, ok := pq.Pop()
			Expect(ok).To(BeTrue())

			_, ok = pq.Pop()
			Expect(ok).To(BeFalse())
		})
	})
})
