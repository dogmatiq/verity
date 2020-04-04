package pdeque_test

import (
	. "github.com/dogmatiq/infix/internal/x/containerx/pdeque"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type elem int

func (e elem) Less(v Elem) bool {
	return e < v.(elem)
}

var _ = Describe("type Deque", func() {
	const (
		elem0 elem = iota
		elem1
		elem2
	)

	var (
		deq *Deque
	)

	BeforeEach(func() {
		deq = &Deque{}
	})

	Describe("func Push()", func() {
		It("adds an element to the queue", func() {
			deq.Push(elem0)
			Expect(deq.Len()).To(Equal(1))
		})

		It("returns true if the new element has the highest priority", func() {
			deq.Push(elem1)

			Expect(deq.Push(elem2)).To(BeFalse())
			Expect(deq.Push(elem0)).To(BeTrue())
		})
	})

	Describe("func PeekFront()", func() {
		It("returns the element with the highest priority", func() {
			deq.Push(elem2)
			deq.Push(elem0)
			deq.Push(elem1)

			e, ok := deq.PeekFront()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem0))

			Expect(deq.Len()).To(
				Equal(3),
				"PeekFront() modified the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := deq.PeekFront()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PopFront()", func() {
		It("removes and returns the element with the highest priority", func() {
			deq.Push(elem2)
			deq.Push(elem0)
			deq.Push(elem1)

			e, ok := deq.PopFront()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem0))

			Expect(deq.Len()).To(
				Equal(2),
				"PopFront() did not modify the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := deq.PopFront()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PeekBack()", func() {
		It("returns the element with the lowest priority", func() {
			deq.Push(elem2)
			deq.Push(elem0)
			deq.Push(elem1)

			e, ok := deq.PeekBack()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem2))

			Expect(deq.Len()).To(
				Equal(3),
				"PeekBack() modified the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := deq.PeekBack()
			Expect(ok).To(BeFalse())
		})
	})

	Describe("func PopBack()", func() {
		It("removes and returns the element with the lowest priority", func() {
			deq.Push(elem2)
			deq.Push(elem0)
			deq.Push(elem1)

			e, ok := deq.PopBack()
			Expect(ok).To(BeTrue())
			Expect(e).To(Equal(elem2))

			Expect(deq.Len()).To(
				Equal(2),
				"PopBack() did not modify the size of the queue",
			)
		})

		It("returns false if the queue is empty", func() {
			_, ok := deq.PopBack()
			Expect(ok).To(BeFalse())
		})
	})
})
