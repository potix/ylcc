package server

import (
        "log"
        "time"
        "net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	Register()
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
		return nil, fmt.Errorf("failed to listen (addrPort = %v): %w", addrPort, err)
	}
	if tlsCertPath != "" && tlsKeyPath != "" {
		serverCred, err := credentials.NewServerTLSFromFile(tlsCertPath, tlsKeyPath)
		if err != nil {
			return nil, fmt.Errorf("can not create server credential (tlsCertPath, tlsKeyPath = %v, $v): %w", tlsCertPath, tlsKeyPath, err)
		}
		grpcServer := grpc.NewServer(grpc.Creds(serverCred))
	} else {
		grpcServer := grpc.NewServer()
	}
	handler.Register(grpcServer)
	newServer := &server{
		verbose: verbose,
		listen: listen,
		grpcServer: grpcServer,
	}
	return newServer, nil
}
