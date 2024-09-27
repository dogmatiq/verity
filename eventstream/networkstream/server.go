package networkstream

import (
	"context"

	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/enginekit/marshaler"
	"github.com/dogmatiq/interopspec/envelopespec"
	"github.com/dogmatiq/interopspec/eventstreamspec"
	"github.com/dogmatiq/marshalkit"
	"github.com/dogmatiq/verity/eventstream"
	"github.com/dogmatiq/verity/internal/x/grpcx"
	"github.com/dogmatiq/verity/parcel"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
)

// ServerOption configures the behavior of a server.
type ServerOption struct {
	key    string
	stream eventstream.Stream
	types  message.TypeCollection
}

// WithApplication returns a server option that configures the server to serve
// events for a specific application.
func WithApplication(
	ak string,
	s eventstream.Stream,
	types message.TypeCollection,
) ServerOption {
	return ServerOption{ak, s, types}
}

// RegisterServer registers an event stream server for the given streams.
func RegisterServer(
	s *grpc.Server,
	m marshaler.Marshaler,
	options ...ServerOption,
) {
	d := &dispatcher{
		apps: map[string]eventstreamspec.StreamAPIServer{},
	}

	for _, opt := range options {
		svr := &server{
			stream:    opt.stream,
			marshaler: m,
			types:     map[string]message.Type{},
			resp:      &eventstreamspec.EventTypesResponse{},
		}

		d.apps[opt.key] = svr

		opt.types.Range(func(mt message.Type) bool {
			rt := mt.ReflectType()
			n := marshalkit.MustMarshalType(m, rt)

			svr.types[n] = mt
			svr.resp.EventTypes = append(
				svr.resp.EventTypes,
				&eventstreamspec.EventType{
					PortableName: n,
					MediaTypes:   m.MediaTypesFor(rt),
				},
			)

			return true
		})
	}

	eventstreamspec.RegisterStreamAPIServer(s, d)
}

// dispatcher is an implementation of the dogma.messaging.v1 EventStream service
// that dispatches to other implementations based on the application key.
type dispatcher struct {
	apps map[string]eventstreamspec.StreamAPIServer
}

func (d *dispatcher) get(k string) (eventstreamspec.StreamAPIServer, error) {
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
			&eventstreamspec.UnrecognizedApplication{ApplicationKey: k},
		},
		"unrecognized application: %s",
		k,
	)
}

func (d *dispatcher) Consume(
	req *eventstreamspec.ConsumeRequest,
	consumer eventstreamspec.StreamAPI_ConsumeServer,
) error {
	s, err := d.get(req.GetApplicationKey())
	if err != nil {
		return err
	}

	return s.Consume(req, consumer)
}

func (d *dispatcher) EventTypes(
	ctx context.Context,
	req *eventstreamspec.EventTypesRequest,
) (*eventstreamspec.EventTypesResponse, error) {
	s, err := d.get(req.GetApplicationKey())
	if err != nil {
		return nil, err
	}

	return s.EventTypes(ctx, req)
}

// server is an implementation of the dogma.messaging.v1 EventStream service for
// a single application.
type server struct {
	stream    eventstream.Stream
	marshaler marshaler.Marshaler
	types     map[string]message.Type
	resp      *eventstreamspec.EventTypesResponse
}

func (s *server) Consume(
	req *eventstreamspec.ConsumeRequest,
	consumer eventstreamspec.StreamAPI_ConsumeServer,
) error {
	ctx := consumer.Context()

	messageTypes, mediaTypes, err := s.unmarshalTypes(req)
	if err != nil {
		return err
	}

	cur, err := s.stream.Open(ctx, req.GetOffset(), messageTypes)
	if err != nil {
		return err
	}
	defer cur.Close()

	for {
		ev, err := cur.Next(ctx)
		if err != nil {
			return err
		}

		env, err := s.transcode(ev.Parcel, mediaTypes)
		if err != nil {
			return err
		}

		res := &eventstreamspec.ConsumeResponse{
			Offset:   ev.Offset,
			Envelope: env,
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
	*eventstreamspec.EventTypesRequest,
) (*eventstreamspec.EventTypesResponse, error) {
	return s.resp, nil
}

// unmarshalTypes unmarshals the event types, and their respective media-types
// from the given request.
func (s *server) unmarshalTypes(
	req *eventstreamspec.ConsumeRequest,
) (
	message.TypeCollection,
	map[string][]string,
	error,
) {
	if len(req.GetEventTypes()) == 0 {
		return nil, nil, grpcx.Errorf(
			codes.InvalidArgument,
			nil,
			"at least one event type must be consumed",
		)
	}

	var (
		messageTypes = message.TypeSet{}
		mediaTypes   = map[string][]string{}
		failures     []proto.Message
	)

	for _, et := range req.GetEventTypes() {
		n := et.GetPortableName()

		if mt, ok := s.types[n]; ok {
			messageTypes.Add(mt)
			mediaTypes[n] = et.GetMediaTypes()
		} else {
			failures = append(
				failures,
				&eventstreamspec.UnrecognizedEventType{PortableName: n},
			)
		}
	}

	if len(failures) > 0 {
		return nil, nil, grpcx.Errorf(
			codes.InvalidArgument,
			failures,
			"one or more unrecognized event types or media-types",
		)
	}

	return messageTypes, mediaTypes, nil
}

// transcode returns an envelope containing the message in p, transcoded into a
// media-type supported by the client.
func (s *server) transcode(
	p parcel.Parcel,
	mediaTypes map[string][]string,
) (*envelopespec.Envelope, error) {
	n := p.Envelope.GetPortableName()
	preferredMediaTypes := mediaTypes[n]

	// First see if the existing format is supported by the client at all. It
	// may not be their first preference, but if it is supported we can send the
	// message packet without marshaling it again.
	for _, mt := range preferredMediaTypes {
		if p.Envelope.GetMediaType() == mt {
			return p.Envelope, nil
		}
	}

	// Otherwise, attempt to marshal the message using the client's requested
	// media-types in order of preference.
	packet, ok, err := s.marshaler.MarshalAs(p.Message, preferredMediaTypes)
	if err != nil {
		return nil, err
	}

	// None of the supplied media-types are supported by the marshaler so
	// there's no way to supply this event to the client.
	if !ok {
		return nil, grpcx.Errorf(
			codes.InvalidArgument,
			[]proto.Message{
				&eventstreamspec.NoRecognizedMediaTypes{
					PortableName: n,
				},
			},
			"none of the requested media-types for '%s' events are supported",
			n,
		)
	}

	env := proto.Clone(p.Envelope).(*envelopespec.Envelope)
	env.MediaType = packet.MediaType
	env.Data = packet.Data

	return env, nil
}
