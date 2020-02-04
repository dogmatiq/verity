package api

import (
	"context"

	"github.com/dogmatiq/configkit"
	"github.com/dogmatiq/infix/api/internal/pb"
	"github.com/dogmatiq/marshalkit"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RegisterMessagingServer registers a pb.MessagingServer with s.
func RegisterMessagingServer(
	s *grpc.Server,
	cfg configkit.RichApplication,
	m marshalkit.Marshaler,
) {
	pb.RegisterMessagingServer(s, &messagingServer{})
}

// messagingServer is an implementation of pb.MessagingServer.
type messagingServer struct{}

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
