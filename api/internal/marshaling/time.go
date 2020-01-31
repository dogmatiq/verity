package marshaling

import "time"

// MarshalTime marshals a time to its protobuf representation.
func MarshalTime(src time.Time) string {
	if src.IsZero() {
		return ""
	}

	data, err := src.MarshalText()
	if err != nil {
		panic(err)
	}

	return string(data)
}

// UnmarshalTime unmarshals a time from its protobuf representation.
func UnmarshalTime(src string, dest *time.Time) error {
	if src == "" {
		*dest = time.Time{}
		return nil
	}

	return dest.UnmarshalText([]byte(src))
}
