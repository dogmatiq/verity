package deque

import (
	"container/heap"
)

// Elem is an element within a double-ended priority queue.
type Elem interface {
	// Less returns true if this element should be closer to the front of the
	// queue than e.
	//
	// Stated another way, it returns true if this element is higher priority
	// than e.
	Less(e Elem) bool
}

// Deque is a double-ended priority queue.
//
// Elements with a higher priority appear at the front of the queue.
type Deque struct {
	min minheap
	max maxheap
}

// Len returns the number of elements on the queue.
func (q *Deque) Len() int {
	return q.min.Len()
}

// Push adds an element to the queue.
//
// It returns true e is at the front of the queue.
func (q *Deque) Push(e Elem) bool {
	n := q.min.Len()
	it := &item{
		elem: e,
		min:  n,
		max:  n,
	}

	heap.Push(&q.min, it)
	heap.Push(&q.max, it)

	return it.min == 0
}

// PeekFront returns the element with the highest priority without removing it
// from the queue.
//
// It returns false if the queue is empty.
func (q *Deque) PeekFront() (Elem, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	return q.min.items[0].elem, true
}

// PopFront removes the element with the highest priority and returns it.
//
// It returns false if the queue is empty.
func (q *Deque) PopFront() (Elem, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	it := heap.Pop(&q.min).(*item)
	heap.Remove(&q.max, it.max)

	return it.elem, true
}

// PeekBack returns the element with the lowest priority without removing it
// from the queue.
//
// It returns false if the queue is empty.
func (q *Deque) PeekBack() (Elem, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	return q.max.items[0].elem, true
}

// Placement returns a value indicating which end of the queue e would appear if
// it were pushed to the queue.
//
// If p < 0, e would be the highest priority element, or the only element.
//
// If p > 0, e would be the lowest priority element.
//
// If p == 0, e would be neither the highest nor lowest priority element.
func (q *Deque) Placement(e Elem) (p int) {
	if len(q.min.items) == 0 {
		return -1
	}

	if e.Less(q.min.items[0].elem) {
		return -1
	}

	if !e.Less(q.max.items[0].elem) {
		return +1
	}

	return 0
}

// PopBack removes the element with the lowest priority and returns it.
//
// It returns false if the queue is empty.
func (q *Deque) PopBack() (Elem, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	it := heap.Pop(&q.max).(*item)
	heap.Remove(&q.min, it.min)

	return it.elem, true
}

// item is an element on the deque.
type item struct {
	elem Elem

	// min is the index of the item within the min-heap.
	min int

	// max is the index of the item within the max-heap.
	max int
}

// qheap is the implementation of the heap common to both the min/max heap.
type qheap struct {
	items []*item
}

func (h *qheap) Len() int {
	return len(h.items)
}

func (h *qheap) Less(i, j int) bool {
	a := h.items[i].elem
	b := h.items[j].elem
	return a.Less(b)
}

func (h *qheap) Push(it interface{}) {
	h.items = append(h.items, it.(*item))
}

func (h *qheap) Pop() interface{} {
	index := len(h.items) - 1
	e := h.items[index]

	h.items[index] = nil // avoid memory leak
	h.items = h.items[:index]

	return e
}

// minheap is a heap where Pop() returns the message with the earliest
// next-attempt time.
type minheap struct{ qheap }

func (h *minheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].min = i
	h.items[j].min = j
}

// maxheap is a heap where Pop() returns the message with the latest
// next-attempt time.
type maxheap struct{ qheap }

func (h *maxheap) Less(i, j int) bool {
	return h.qheap.Less(j, i)
}

func (h *maxheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].max = i
	h.items[j].max = j
}
