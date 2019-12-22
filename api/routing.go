package api

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
	"google.golang.org/grpc"
)

// RegisterRoutingServer registers a pb.RoutingServer with s.
func RegisterRoutingServer(
	s *grpc.Server,
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) {
	rs := &routingServer{
		application: marshalIdentity(cfg.Identity()),
		commands:    map[string]struct{}{},
		events:      map[string]struct{}{},
	}

	types := cfg.MessageTypes()

	for t, r := range types.Consumed {
		if r == message.CommandRole {
			mt := marshalkit.MustMarshalType(m, t.ReflectType())
			rs.commands[mt] = struct{}{}
		}
	}

	for t, r := range types.Produced {
		if r == message.EventRole {
			mt := marshalkit.MustMarshalType(m, t.ReflectType())
			rs.events[mt] = struct{}{}
		}
	}

	pb.RegisterRoutingServer(s, rs)
}

// routingServer is an implementation of pb.RoutingServer.
type routingServer struct {
	application *pb.Identity
	commands    map[string]struct{}
	events      map[string]struct{}
}

func (s *routingServer) DiscoverRoutes(
	ctx context.Context,
	req *pb.DiscoverRoutesRequest,
) (*pb.DiscoverRoutesResponse, error) {
	route := &pb.ApplicationRoute{
		Application: s.application,
	}

	for _, mt := range req.Commands {
		if _, ok := s.commands[mt]; ok {
			route.Commands = append(route.Commands, mt)
		}
	}

	for _, mt := range req.Events {
		if _, ok := s.events[mt]; ok {
			route.Events = append(route.Events, mt)
		}
	}

	res := &pb.DiscoverRoutesResponse{}

	if len(req.Commands)+len(req.Events) > 0 {
		res.Routes = append(res.Routes, route)
	}

	return res, nil
}
