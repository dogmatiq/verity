package api

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/envelope"
	"github.com/dogmatiq/infix/internal/draftspecs/messagingspec"
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

	messagingspec.RegisterEventStreamServer(s, svr)
}

type streamServer struct {
	marshaler marshalkit.TypeMarshaler
	streams   map[string]persistence.Stream
}

func (s *streamServer) Consume(
	req *messagingspec.ConsumeRequest,
	consumer messagingspec.EventStream_ConsumeServer,
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

		res := &messagingspec.ConsumeResponse{
			Offset:   m.Offset,
			Envelope: envelope.MustMarshal(m.Envelope),
		}

		res.Envelope, err = envelope.Marshal(m.Envelope)
		if err != nil {
			return err
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
	req *messagingspec.ConsumeRequest,
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
			&messagingspec.UnrecognizedApplication{ApplicationKey: k},
		},
		"unrecognized application: %s",
		k,
	)
}

// unmarshalMessageTypes unmarshals a collection of message types from their
// protocol buffers representation.
func unmarshalMessageTypes(
	m marshalkit.TypeMarshaler,
	in []string,
) (message.TypeSet, error) {
	out := message.TypeSet{}

	var failed []proto.Message

	for _, n := range in {
		rt, err := m.UnmarshalType(n)
		if err != nil {
			failed = append(
				failed,
				&messagingspec.UnrecognizedMessage{Name: n},
			)
		} else {
			t := message.TypeFromReflect(rt)
			out[t] = struct{}{}
		}
	}

	if len(failed) > 0 {
		return nil, errorf(
			codes.InvalidArgument,
			failed,
			"unrecognized message type(s)",
		)
	}

	if len(out) == 0 {
		return nil, errorf(
			codes.InvalidArgument,
			nil,
			"message types can not be empty",
		)
	}

	return out, nil
}
