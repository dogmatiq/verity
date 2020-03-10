package grpcx

import (
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Errorf returns a new gRPC status error, with an optional detail messages.
func Errorf(
	code codes.Code,
	details []proto.Message,
	f string,
	v ...interface{},
) error {
	s := status.Newf(code, f, v...)

	var err error
	s, err = s.WithDetails(details...)
	if err != nil {
		panic(err)
	}

	return s.Err()
}
