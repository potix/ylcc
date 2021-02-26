package server

import (
        "log"
        "time"
        "net"
	"google.golang.org/grpc"
)

// Server is server
type Server struct {
	verbose         bool
	listen          *net.Listener
        grpcServer      *grpc.Server
	handler         Handler
}

// Handler is handler interface
type Handler insterface {
	Start()
	Stop()
	YlccServer
}

func (s *server) Start() {
	s.handler.Start()
        go func() {
		if err := s.grpcServer.Serve(lis); err != nil {
			log.Fatalf("can not create server credential: %w", err)
		}
	}
}

func (s *server) Stop() {
	s.grpcServer.Stop()
	s.handler.Stop()
}

// NewServer is http server with tls 
func NewServer(verbose bool, addrPort string, tlsCertPath string, tlsKeyPath string, handler Handler) (*server, error) {
	listen, err := net.Listen("tcp", addrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}
	serverCred, err := credentials.NewServerTLSFromFile(tlsCertPath, tlsKeyPath)
	if err != nil {
		return nil, fmt.Errorf("can not create server credential: %w", err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(serverCred))
	newServer := &server{
		verbose: verbose,
		listen: listen,
		grpcServer: grpcServer,
	}
	pb.RegisterYlccServer(grpcServer, handler)
	return newServer, nil
}
