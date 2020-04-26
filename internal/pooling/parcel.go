package pooling

import (
	"sync"

	"github.com/dogmatiq/infix/parcel"
)

// ParcelSlices is a pool of []*parcel.Parcel.
var ParcelSlices parcelSlice

type parcelSlice sync.Pool

// Get returns a slice with capacity c.
func (p *parcelSlice) Get(c int) []*parcel.Parcel {
	if v := (*sync.Pool)(p).Get(); v != nil {
		s := v.([]*parcel.Parcel)
		if cap(s) >= c {
			return s
		}
	}

	return make([]*parcel.Parcel, 0, c)
}

// Put adds v to the pool.
func (p *parcelSlice) Put(v []*parcel.Parcel) {
	(*sync.Pool)(p).Put(v[:0])
}
