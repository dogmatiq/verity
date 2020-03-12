package streamfilter

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/marshalkit"
)

// Hash marshals the given message types to their "portable name" and computes a
// deterministic hash of those names.
func Hash(
	m marshalkit.Marshaler,
	types message.TypeCollection,
) ([]byte, []string) {
	var names []string

	types.Range(func(t message.Type) bool {
		names = append(
			names,
			marshalkit.MustMarshalType(
				m,
				t.ReflectType(),
			),
		)

		return true
	})

	sort.Strings(names)

	hash := sha256.New()
	for _, n := range names {
		hash.Write([]byte(n))
	}

	b := hash.Sum(nil)
	x := make([]byte, hex.EncodedLen(len(b)))
	hex.Encode(x, b)

	return x, names
}

// CompareNames returns true if the two slices of names are equal.
//
// It is assumed both slices are already sorted.
func CompareNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i, n := range a {
		if n != b[i] {
			return false
		}
	}

	return true
}
