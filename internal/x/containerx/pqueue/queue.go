package pqueue

import (
	"container/heap"
)

// Elem is an element within a priority queue.
type Elem interface {
	// Less returns true if this element should be closer to the front of the
	// queue than e.
	//
	// Stated another way, it returns true if this element is higher priority
	// than e.
	Less(e Elem) bool
}

// Queue is a single-ended priority-queue that allows updating and removal of
// arbitrary elements.
//
// Elements with a higher priority appear at the front of the queue.
type Queue struct {
	heap  qheap
	items map[Elem]*item
}

// Len returns the number of elements on the queue.
func (q *Queue) Len() int {
	return q.heap.Len()
}

// Push adds an element to the queue.
//
// It return strue if e is at the front of the queue.
func (q *Queue) Push(e Elem) bool {
	n := q.heap.Len()

	it := &item{
		elem:  e,
		index: n,
	}

	if q.items == nil {
		q.items = map[Elem]*item{}
	}

	q.items[e] = it
	heap.Push(&q.heap, it)

	return it.index == 0
}

// Peek returns the element with the highest priority without removing it from
// the queue.
//
// It returns false if the queue is empty.
func (q *Queue) Peek() (Elem, bool) {
	if q.heap.Len() == 0 {
		return nil, false
	}

	return q.heap.items[0].elem, true
}

// Pop removes the element with the highest priority and returns it.
//
// It returns false if the queue is empty.
func (q *Queue) Pop() (Elem, bool) {
	if q.heap.Len() == 0 {
		return nil, false
	}

	it := heap.Pop(&q.heap).(*item)

	return it.elem, true
}

// Remove removes e from the queue.
//
// It returns false if e is not on the queue.
func (q *Queue) Remove(e Elem) bool {
	if it, ok := q.items[e]; ok {
		heap.Remove(&q.heap, it.index)
		return true
	}

	return false
}

// Update reorders the queue after the priority of e has changed.
//
// It returns false if e is not on the queue.
func (q *Queue) Update(e Elem) bool {
	if it, ok := q.items[e]; ok {
		heap.Fix(&q.heap, it.index)
		return true
	}

	return false
}

// item is an element on the deque.
type item struct {
	elem Elem

	// id is the element's unique ID.
	id int

	// the index of the item within the heap.
	index int
}

// qheap is the implementation heap.Interface.
type qheap struct {
	items []*item
}

func (h *qheap) Len() int {
	return len(h.items)
}

func (h *qheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
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
