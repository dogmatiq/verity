package eventstream

// A Publisher informs observers when streams become available or unavailable.
type Publisher interface {
	// RegisterStreamObserver registers an observer to be notified of updates to
	// stream availability.
	//
	// The observer is immediately notified of the available streams.
	RegisterStreamObserver(Observer)

	// UnregisterStreamObserver stops an observer from being notified of updates
	// to stream availability.
	UnregisterStreamObserver(Observer)
}

// An Observer is notified by a Publisher when streams become available or
// unavailable.
type Observer interface {
	// StreamAvailable notifies the observer that a stream has become available.
	StreamAvailable(Stream)

	// StreamAvailable notifies the observer that a stream is no longer
	// available.
	StreamUnavailable(Stream)
}
