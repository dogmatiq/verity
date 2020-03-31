package processor

import (
	"container/heap"

	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// pqueue is a double-ended priority queue.
//
// Messages are prioritized according to their "next attempt" time. The message
// that is to be attempted soonest is said to have the hightest priority.
type pqueue struct {
	min minheap
	max maxheap
}

// Len returns the number of messages on the queue.
func (q *pqueue) Len() int {
	return q.min.Len()
}

// Push adds a message to the queue.
func (q *pqueue) Push(m *queue.Message) {
	n := q.min.Len()
	i := &item{
		message: m,
		min:     n,
		max:     n,
	}

	heap.Push(&q.min, i)
	heap.Push(&q.max, i)
}

// PeekFront returns the message with the highest priority without removing it
// from the queue.
//
// It returns false if the queue is empty.
func (q *pqueue) PeekFront() (*queue.Message, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	return q.min.items[0].message, true
}

// PopFront removes the message with the highest priority and returns it.
//
// It returns false if the queue is empty.
func (q *pqueue) PopFront() (*queue.Message, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	i := heap.Pop(&q.min).(*item)
	heap.Remove(&q.max, i.max)

	return i.message, true
}

// PeekBack returns the message with the lowest priority without removing it
// from the queue.
//
// It returns false if the queue is empty.
func (q *pqueue) PeekBack() (*queue.Message, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	return q.max.items[0].message, true
}

// PopBack removes the message with the lowest priority and returns it.
//
// It returns false if the queue is empty.
func (q *pqueue) PopBack() (*queue.Message, bool) {
	if q.min.Len() == 0 {
		return nil, false
	}

	i := heap.Pop(&q.max).(*item)
	heap.Remove(&q.min, i.min)

	return i.message, true
}

// item is a message on the priority queue.
type item struct {
	// message is the queued message.
	message *queue.Message

	// min is the index of the item within the min-heap.
	min int

	// max is the index of the item within the max-heap.
	max int
}

// pheap is the implementation of the heap common to both the min/max heap.
type pheap struct {
	items []*item
}

func (h *pheap) Len() int {
	return len(h.items)
}

func (h *pheap) Less(i, j int) bool {
	a := h.items[i].message.NextAttemptAt
	b := h.items[j].message.NextAttemptAt
	return a.Before(b)
}

func (h *pheap) Push(v interface{}) {
	h.items = append(h.items, v.(*item))
}

func (h *pheap) Pop() interface{} {
	index := len(h.items) - 1
	m := h.items[index]

	h.items[index] = nil // avoid memory leak
	h.items = h.items[:index]

	return m
}

// minheap is a heap where Pop() returns the message with the earliest
// next-attempt time.
type minheap struct{ pheap }

func (h *minheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].min = i
	h.items[j].min = j
}

// maxheap is a heap where Pop() returns the message with the latest
// next-attempt time.
type maxheap struct{ pheap }

func (h *maxheap) Less(i, j int) bool {
	return h.pheap.Less(j, i)
}

func (h *maxheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].max = i
	h.items[j].max = j
}
