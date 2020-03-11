package bboltx

// Recover recovers from a panic caused by one of the MustXXX() functions.
//
// It is intended to be used in a defer statement. The error that caused the
// panic is assigned to *err.
func Recover(err *error) {
	if err == nil {
		panic("err must be a non-nil pointer")
	}

	switch v := recover().(type) {
	case PanicSentinel:
		*err = v.Cause
	case nil:
		return
	default:
		panic(v)
	}
}

// PanicSentinel is a wrapper value used to identify panic's that are caused
// by one of the MustXXX() functions.
type PanicSentinel struct {
	// Cause is the error that caused the panic.
	Cause error
}

// Must panics if err is non-nil.
func Must(err error) {
	if err != nil {
		panic(PanicSentinel{err})
	}
}
