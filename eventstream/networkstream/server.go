package networkstream

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/draftspecs/messagingspec"
	"github.com/dogmatiq/infix/eventstream"
	"github.com/dogmatiq/infix/internal/x/grpcx"
	"github.com/dogmatiq/marshalkit"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

// ServerOption configures the behavior of a server.
type ServerOption struct {
	key    string
	stream eventstream.Stream
	types  message.TypeCollection
}

// WithApplication returns a server option that configures the server to server
// events for a specific application.
func WithApplication(
	ak string,
	s eventstream.Stream,
	types message.TypeCollection,
) ServerOption {
	return ServerOption{ak, s, types}
}

// RegisterServer registers an event stream server for the given streams.
//
// repos is a map of application key to the eventstore.Repository for that
// application.
func RegisterServer(
	s *grpc.Server,
	m marshalkit.TypeMarshaler,
	options ...ServerOption,
) {
	d := &dispatcher{
		apps: map[string]messagingspec.EventStreamServer{},
	}

	for _, opt := range options {
		svr := &server{
			stream: opt.stream,
			types:  map[string]message.Type{},
			resp:   &messagingspec.MessageTypesResponse{},
		}

		d.apps[opt.key] = svr

		opt.types.Range(func(mt message.Type) bool {
			n := marshalkit.MustMarshalType(m, mt.ReflectType())

			svr.types[n] = mt
			svr.resp.MessageTypes = append(
				svr.resp.MessageTypes,
				&messagingspec.MessageType{
					PortableName: n,
					ConfigName:   mt.Name().String(),
					// TODO: https://github.com/dogmatiq/infix/issues/49
					// Populate supported MIME media-types.
					MediaTypes: nil,
				},
			)

			return true
		})
	}

	messagingspec.RegisterEventStreamServer(s, d)
}

// dispatcher is an implementation of the dogma.messaging.v1 EventStream service
// that dispatches to other implementations based on the application key.
type dispatcher struct {
	apps map[string]messagingspec.EventStreamServer
}

func (d *dispatcher) get(k string) (messagingspec.EventStreamServer, error) {
	if k == "" {
		return nil, grpcx.Errorf(
			codes.InvalidArgument,
			nil,
			"application key must not be empty",
		)
	}

	if s, ok := d.apps[k]; ok {
		return s, nil
	}

	return nil, grpcx.Errorf(
		codes.NotFound,
		[]proto.Message{
			&messagingspec.UnrecognizedApplication{ApplicationKey: k},
		},
		"unrecognized application: %s",
		k,
	)
}

func (d *dispatcher) Consume(
	req *messagingspec.ConsumeRequest,
	consumer messagingspec.EventStream_ConsumeServer,
) error {
	s, err := d.get(req.GetApplicationKey())
	if err != nil {
		return err
	}

	return s.Consume(req, consumer)
}

func (d *dispatcher) EventTypes(
	ctx context.Context,
	req *messagingspec.MessageTypesRequest,
) (*messagingspec.MessageTypesResponse, error) {
	s, err := d.get(req.GetApplicationKey())
	if err != nil {
		return nil, err
	}

	return s.EventTypes(ctx, req)
}

// server is an implementation of the dogma.messaging.v1 EventStream service for
// a single application.
type server struct {
	stream eventstream.Stream
	types  map[string]message.Type
	resp   *messagingspec.MessageTypesResponse
}

func (s *server) Consume(
	req *messagingspec.ConsumeRequest,
	consumer messagingspec.EventStream_ConsumeServer,
) error {
	ctx := consumer.Context()

	types, err := s.unmarshalTypes(req)
	if err != nil {
		return err
	}

	cur, err := s.stream.Open(ctx, req.GetOffset(), types)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		ev, err := cur.Next(ctx)
		if err != nil {
			return err
		}

		res := &messagingspec.ConsumeResponse{
			Offset:   ev.Offset,
			Envelope: ev.Parcel.Envelope,
		}

		if err := consumer.Send(res); err != nil {
			// CODE COVERAGE: It's difficult to get the server to fail to
			// send, possibly because of the outbound network buffer, or
			// some in-process buffering on the server side.
			return err
		}
	}
}

func (s *server) EventTypes(
	context.Context,
	*messagingspec.MessageTypesRequest,
) (*messagingspec.MessageTypesResponse, error) {
	return s.resp, nil
}

func (s *server) unmarshalTypes(req *messagingspec.ConsumeRequest) (message.TypeCollection, error) {
	var (
		types  = message.TypeSet{}
		failed []proto.Message
	)

	for _, n := range req.GetTypes() {
		if mt, ok := s.types[n]; ok {
			types.Add(mt)
		} else {
			failed = append(
				failed,
				&messagingspec.UnrecognizedMessage{Name: n},
			)
		}
	}

	if len(failed) > 0 {
		return nil, grpcx.Errorf(
			codes.InvalidArgument,
			failed,
			"unrecognized message type(s)",
		)
	}

	if len(types) == 0 {
		return nil, grpcx.Errorf(
			codes.InvalidArgument,
			nil,
			"message types can not be empty",
		)
	}

	return types, nil
}
