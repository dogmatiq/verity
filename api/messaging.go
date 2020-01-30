package api

import (
	"context"

	"google.golang.org/grpc/codes"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/configkit/message"
	"github.com/dogmatiq/infix/api/internal/marshaling"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// RegisterMessagingServer registers a pb.MessagingServer with s.
func RegisterMessagingServer(
	s *grpc.Server,
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) {
	var ms messagingServer

	app := &pb.Application{
		Identity: marshaling.MarshalIdentity(cfg.Identity()),
	}

	types := cfg.MessageTypes()

	for t, r := range types.Consumed {
		if r == message.CommandRole {
			mt := marshalkit.MustMarshalType(m, t.ReflectType())
			app.Commands = append(app.Commands, mt)
		}
	}

	for t, r := range types.Produced {
		if r == message.EventRole {
			mt := marshalkit.MustMarshalType(m, t.ReflectType())
			app.Events = append(app.Events, mt)
		}
	}

	// ms.ListApplications.init(cfg, m)

	pb.RegisterMessagingServer(s, &ms)
}

// messagingServer is an implementation of pb.MessagingServer.
type messagingServer struct {
	applications pb.ListApplicationsResponse
}

func (s *messagingServer) ListApplications(
	ctx context.Context,
	_ *pb.ListApplicationsRequest,
) (*pb.ListApplicationsResponse, error) {
	return &s.applications, nil
}

func (s *messagingServer) ExecuteCommand(
	ctx context.Context,
	req *pb.ExecuteCommandRequest,
) (*pb.ExecuteCommandResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "not implemented")
}

func (s *messagingServer) ConsumeEvents(
	req *pb.ConsumeEventsRequest,
	svr pb.Messaging_ConsumeEventsServer,
) error {
	return status.Errorf(codes.Unimplemented, "not implemented")
}
