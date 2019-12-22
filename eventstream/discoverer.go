package eventstream

// Discoverer is an interface for discovering event streams.
type Discoverer interface {
	// Subscribe registers a subscriber with the discoverer, causing it to be
	// notified of any changes to the set of known event streams.
	Subscribe(DiscoverySubscriber)

	// Unsubscribe removes a subscriber from the discoverer, stopping it from
	// being notified of any changes to the set of known event streams.
	Unsubscribe(DiscoverySubscriber)
}

// DiscoverySubscriber is an interface that is notified by a discoverer when an
// event stream is "discovered" or "undiscovered".
type DiscoverySubscriber interface {
	// Discovered is called by a discover when an event stream is discovered.
	Discovered(Stream)

	// Undiscovered is called by a discover when an event stream is undiscovered.
	Undiscovered(Stream)
}
