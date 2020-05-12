package handler

import (
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/queue"
)

// Result is the result of a successful unit-of-work.
type Result struct {
	// Events is the set of events that were recorded in the unit-of-work.
	Events []eventstream.Event

	// Queued is the set of messages that were placed on the message queue,
	// which may include events.
	Queued []queue.Message
}

// Observer is a function that is notified of the result of a unit-of-work.
type Observer func(Result, error)

// NotifyObservers notifies the observers in w.
func NotifyObservers(w *UnitOfWork, res Result, err error) {
	for _, obs := range w.observers {
		obs(res, err)
	}
}
