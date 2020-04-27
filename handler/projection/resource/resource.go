package resource

import (
	"encoding/binary"
	"fmt"
)

// FromApplicationKey returns the resource to use for the given application with
// the given identity key.
func FromApplicationKey(k string) []byte {
	return []byte(k)
}

// MarshalOffset marshals a stream offset to a resource version.
//
// o is the next offset to be read from the stream, not the last offset
// that was applied to the projection.
func MarshalOffset(o uint64) []byte {
	return MarshalOffsetInto(make([]byte, 8), o)
}

// MarshalOffsetInto marshals a stream offset using the memory of an existing
// buffer.
//
// buf is the buffer to use, it must be at least 8 bytes in length.
//
// o is the next offset to be read from the stream, not the last offset that was
// applied to the projection.
func MarshalOffsetInto(buf []byte, o uint64) []byte {
	if o == 0 {
		return buf[:0]
	}

	binary.BigEndian.PutUint64(buf, uint64(o)-1)

	return buf[:8]
}

// UnmarshalOffset unmarshals a stream offset from a resource version.
//
// It returns the next offset to be read from the stream, not the last offset
// that was applied to the projection.
func UnmarshalOffset(v []byte) (uint64, error) {
	switch len(v) {
	case 0:
		return 0, nil
	case 8:
		return binary.BigEndian.Uint64(v) + 1, nil
	default:
		return 0, fmt.Errorf(
			"version is %d byte(s), expected 0 or 8",
			len(v),
		)
	}
}
