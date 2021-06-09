package grpcx

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/runtime/protoiface"
	"google.golang.org/protobuf/runtime/protoimpl"
)

// Errorf returns a new gRPC status error, with an optional detail messages.
func Errorf(
	code codes.Code,
	details []proto.Message,
	f string,
	v ...interface{},
) error {
	s := status.Newf(code, f, v...)

	detailsV1 := make([]protoiface.MessageV1, len(details))

	for i, m := range details {
		detailsV1[i] = protoimpl.X.ProtoMessageV1Of(m)
	}

	var err error
	s, err = s.WithDetails(detailsV1...)
	if err != nil {
		panic(err)
	}

	return s.Err()
}
