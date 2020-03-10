package api

import (
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// errorf returns a new gRPC status error, with an optional detail messages.
func errorf(
	code codes.Code,
	details []proto.Message,
	f string,
	v ...interface{},
) error {
	s := status.Newf(code, f, v...)

	var err error
	s, err = s.WithDetails(details...)
	if err != nil {
		// CODE COVERAGE: This would only happen if we passed a success code for
		// an error... so that should never happen, right?
		panic(err)
	}

	return s.Err()
}
