package server

import (
        "fmt"
        "log"
        "net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Server is server
type Server struct {
	verbose         bool
	listen          net.Listener
        grpcServer      *grpc.Server
	handler         Handler
}

// Handler is handler interface
type Handler interface {
	Start() (error)
	Stop()
	Register(*grpc.Server)
}

func (s *Server) Start() (error) {
	if err := s.handler.Start(); err != nil {
		return fmt.Errorf("can not start handler: %w", err)
	}
        go func() {
		if err := s.grpcServer.Serve(s.listen); err != nil {
			log.Fatalf("can not create server credential: %w", err)
		}
	}()
	return nil
}

func (s *Server) Stop() {
	s.grpcServer.Stop()
	s.handler.Stop()
}

// NewServer is http server with tls 
func NewServer(verbose bool, addrPort string, tlsCertPath string, tlsKeyPath string, handler Handler) (*Server, error) {
	listen, err := net.Listen("tcp", addrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen (addrPort = %v): %w", addrPort, err)
	}
	grpcServer := grpc.NewServer()
	if tlsCertPath != "" && tlsKeyPath != "" {
		serverCred, err := credentials.NewServerTLSFromFile(tlsCertPath, tlsKeyPath)
		if err != nil {
			return nil, fmt.Errorf("can not create server credential (tlsCertPath, tlsKeyPath = %v, $v): %w", tlsCertPath, tlsKeyPath, err)
		}
		grpcServer = grpc.NewServer(grpc.Creds(serverCred))
	}
	handler.Register(grpcServer)
	newServer := &Server{
		verbose: verbose,
		listen: listen,
		grpcServer: grpcServer,
		handler: handler,
	}
	return newServer, nil
}
