package pqueue_test

import (
	. "github.com/dogmatiq/infix/internal/x/containerx/pqueue"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type elem struct {
	priority int
}

func (e *elem) Less(v Elem) bool {
	return e.priority < v.(*elem).priority
}

var _ = Describe("type Queue", func() {
	var (
		elem0, elem1, elem2 *elem
		queue               *Queue
	)

	BeforeEach(func() {
		elem0 = &elem{priority: 0}
		elem1 = &elem{priority: 1}
		elem2 = &elem{priority: 2}

		queue = &Queue{}
	})

	Describe("func Push()", func() {
		It("adds an element to the queue", func() {
			queue.Push(elem0)
			Expect(queue.Len()).To(Equal(1))
		})

		It("returns true if the new element has the highest priority", func() {
			queue.Push(elem1)

			Expect(queue.Push(elem2)).To(BeFalse())
			Expect(queue.Push(elem0)).To(BeTrue())
		})
	})

	Describe("func Peek()", func() {
		It("returns the element with the highest priority", func() {
			queue.Push(elem2)
			queue.Push(elem0)
			queue.Push(elem1)

			e, ok := queue.Peek()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem0))

			Expect(queue.Len()).To(
				Equal(3),
				"Peek() modified the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := queue.Peek()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func Pop()", func() {
		It("removes and returns the element with the highest priority", func() {
			queue.Push(elem2)
			queue.Push(elem0)
			queue.Push(elem1)

			e, ok := queue.Pop()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem0))

			Expect(queue.Len()).To(
				Equal(2),
				"Pop() did not modify the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := queue.Pop()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func Remove()", func() {
		It("removes the element from the queue", func() {
			queue.Push(elem2)
			queue.Push(elem0)
			queue.Push(elem1)

			ok := queue.Remove(elem1)
			Expect(ok).To(BeTrue())

			e, ok := queue.Pop()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem0))

			e, ok = queue.Pop()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem2))
		})

		It("returns false if the element is not on the queue", func() {
			queue.Push(elem2)
			queue.Push(elem0)

			ok := queue.Remove(elem1)
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func Update()", func() {
		It("reorders the queue to match the new priority", func() {
			queue.Push(elem2)
			queue.Push(elem0)
			queue.Push(elem1)

			elem1.priority = -100
			ok := queue.Update(elem1)
			Expect(ok).To(BeTrue())

			e, ok := queue.Pop()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem1))
		})

		It("returns false if the element is not on the queue", func() {
			queue.Push(elem2)
			queue.Push(elem0)

			ok := queue.Update(elem1)
			Expect(ok).To(BeFalse())
		})
	})
})
