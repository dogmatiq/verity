package api

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
)

// discoverRoutes is an provides the implementation of the DiscoverRoutes()
// message of pb.MessagingServer.
type discoverRoutes struct {
	application *pb.Identity
	commands    map[string]struct{}
	events      map[string]struct{}
}

func (s *discoverRoutes) DiscoverRoutes(
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

func (s *discoverRoutes) init(
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) {
	types := cfg.MessageTypes()

	for t, r := range types.Consumed {
		if r == message.CommandRole {
			mt := marshalkit.MustMarshalType(m, t.ReflectType())
			s.commands[mt] = struct{}{}
		}
	}

	for t, r := range types.Produced {
		if r == message.EventRole {
			mt := marshalkit.MustMarshalType(m, t.ReflectType())
			s.events[mt] = struct{}{}
		}
	}
}
