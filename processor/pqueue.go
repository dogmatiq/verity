package processor

import (
	"container/heap"
	"sync"

	"github.com/dogmatiq/infix/persistence/subsystem/queue"
)

// pqueue is a capped-size, in-memory priority queue of messages.
//
// Messages are prioritized according to their "next attempt" time.
type pqueue struct {
	limit int

	m   sync.Mutex
	min minheap
	max maxheap
}

// Push adds a message to the queue.
//
// If the new message causes the queue to exceed its size limit, the lowest
// priority message is removed.
func (q *pqueue) Push(m *queue.Message) {
	if q.limit == 0 {
		return
	}

	q.m.Lock()
	defer q.m.Unlock()

	n := q.min.Len()
	pop := n == q.limit

	i := &item{
		message: m,
		min:     n,
		max:     n,
	}

	heap.Push(&q.min, i)
	heap.Push(&q.max, i)

	if pop {
		i := heap.Pop(&q.max).(*item)
		heap.Remove(&q.min, i.min)
	}
}

// Pop removes the next message from the queue.
//
// It returns false if the queue is empty.
func (q *pqueue) Pop() (*queue.Message, bool) {
	q.m.Lock()
	defer q.m.Unlock()

	if q.min.Len() == 0 {
		return nil, false
	}

	i := heap.Pop(&q.min).(*item)
	heap.Remove(&q.max, i.max)

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

// minheap is a heap that pop's messages with the earliest next-attempt times.
type minheap struct{ pheap }

func (h *minheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].min = i
	h.items[j].min = j
}

// maxheap is a heap that pop's messages with the latest next-attempt times.
type maxheap struct{ pheap }

func (h *maxheap) Less(i, j int) bool {
	return h.pheap.Less(j, i)
}

func (h *maxheap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].max = i
	h.items[j].max = j
}
