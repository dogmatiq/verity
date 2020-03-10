package api

import (
	"context"

	"github.com/dogmatiq/infix/internal/draftspecs/messaging/pb"
	"github.com/dogmatiq/infix/persistence"
	"github.com/dogmatiq/marshalkit"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// RegisterEventStreamServer registers an event stream server for the given
// streams.
//
// streams is a map of application key to the stream of that application.
func RegisterEventStreamServer(
	s *grpc.Server,
	m marshalkit.TypeMarshaler,
	streams map[string]persistence.Stream,
) {
	svr := &streamServer{
		marshaler: m,
		streams:   streams,
	}

	pb.RegisterEventStreamServer(s, svr)
}

type streamServer struct {
	marshaler marshalkit.TypeMarshaler
	streams   map[string]persistence.Stream
}

func (s *streamServer) Consume(
	req *pb.ConsumeRequest,
	consumer pb.EventStream_ConsumeServer,
) error {
	ctx := consumer.Context()

	cur, err := s.open(ctx, req)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		m, err := cur.Next(ctx)
		if err != nil {
			return err
		}

		res := &pb.ConsumeResponse{
			Offset:   m.Offset,
			Envelope: marshalEnvelope(m.Envelope),
		}

		if err := consumer.Send(res); err != nil {
			// CODE COVERAGE: It's difficult to get the server to fail to send,
			// possibly because of the outbound network buffer, or some
			// in-process buffering on the server side.
			return err
		}
	}
}

// open returns a cursor for the stream specified in the request.
func (s *streamServer) open(
	ctx context.Context,
	req *pb.ConsumeRequest,
) (persistence.StreamCursor, error) {
	stream, err := s.stream(req.ApplicationKey)
	if err != nil {
		return nil, err
	}

	types, err := unmarshalMessageTypes(s.marshaler, req.GetTypes())
	if err != nil {
		return nil, err
	}

	return stream.Open(
		ctx,
		req.Offset,
		types,
	)
}

// stream returns the stream for the application with the specified key.
func (s *streamServer) stream(k string) (persistence.Stream, error) {
	if k == "" {
		return nil, errorf(
			codes.InvalidArgument,
			nil,
			"application key must not be empty",
		)
	}

	if stream, ok := s.streams[k]; ok {
		return stream, nil
	}

	return nil, errorf(
		codes.NotFound,
		[]proto.Message{
			&pb.UnrecognizedApplication{ApplicationKey: k},
		},
		"unrecognized application: %s",
		k,
	)
}
