package api

import "time"

// marshalTime marshals a time to its protobuf representation.
func marshalTime(src time.Time) string {
	if src.IsZero() {
		return ""
	}

	data, err := src.MarshalText()
	if err != nil {
		panic(err)
	}

	return string(data)
}

// unmarshalTime unmarshals a time from its protobuf representation.
func unmarshalTime(src string, dest *time.Time) error {
	if src == "" {
		*dest = time.Time{}
		return nil
	}

	return dest.UnmarshalText([]byte(src))
}
