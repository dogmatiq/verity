package sqlx

import "time"

// MarshalTime marshals a time into a human-readable textual representation.
func MarshalTime(t time.Time) []byte {
	if t.IsZero() {
		return nil
	}

	data, err := t.MarshalText()
	Must(err)

	return data
}

// UnmarshalTime unmarshals a time from its human-readable textual
// representation.
func UnmarshalTime(data []byte) time.Time {
	var t time.Time

	if len(data) > 0 {
		Must(t.UnmarshalText(data))
	}

	return t
}
