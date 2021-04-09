package server

import (
        "fmt"
        "log"
        "net"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type options struct {
	verbose     bool
	tlsCertPath string
	tlsKeyPath  string
}

type Option func(*options)

func Verbose(verbose bool) Option {
	return func(opts *options) {
		options.verbose = verbose
	}
}

func TLS(tlsCertPath string, tlsKeyPath string) Option {
	return func(opts *options) {
		options.tlsCertPath = tlsCertPath
		options.tlsKeyPath = tlsKeyPath
	}
}

type Server struct {
	verbose         bool
	listen          net.Listener
        grpcServer      *grpc.Server
	handler         Handler
}

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

func NewServer(addrPort string,  handler Handler, options ...Option) (*Server, error) {
	opts := &pptions{
		verbose: false,
		tlsCertPath: "",
		tlsKeyPath: "",
	}
	for _, opt := range options {
		opt(opts)
	}
	listen, err := net.Listen("tcp", addrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen (addrPort = %v): %w", addrPort, err)
	}
	grpcServer := grpc.NewServer()
	if opts.tlsCertPath != "" && opts.tlsKeyPath != "" {
		serverCred, err := credentials.NewServerTLSFromFile(opts.tlsCertPath, optstlsKeyPath)
		if err != nil {
			return nil, fmt.Errorf("can not create server credential (tlsCertPath, tlsKeyPath = %v, $v): %w", opts.tlsCertPath, optstlsKeyPath, err)
		}
		grpcServer = grpc.NewServer(grpc.Creds(serverCred))
	}
	handler.Register(grpcServer)
	newServer := &Server{
		verbose: opts.verbose,
		listen: listen,
		grpcServer: grpcServer,
		handler: handler,
	}
	return newServer, nil
}
