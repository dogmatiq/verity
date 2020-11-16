package boltdb

import (
	"encoding/binary"
	"fmt"

	"github.com/dogmatiq/verity/internal/x/bboltx"
)

// marshalUint64 marshals a uint64 to its binary representation.
func marshalUint64(n uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, n)
	return data
}

// unmarshalUint64 unmarshals a uint64 from its binary representation.
func unmarshalUint64(data []byte) uint64 {
	n := len(data)

	switch n {
	case 0:
		return 0
	case 8:
		return binary.BigEndian.Uint64(data)
	default:
		panic(bboltx.PanicSentinel{
			Cause: fmt.Errorf("data is corrupt, expected 8 bytes, got %d", n),
		})
	}
}
