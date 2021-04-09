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

func defaultOptions() (*options) {
        return &options {
		verbose: false,
		tlsCertPath: "",
		tlsKeyPath: "",
	}
}

type Option func(*options)

func Verbose(verbose bool) Option {
	return func(opts *options) {
		opts.verbose = verbose
	}
}

func TLS(tlsCertPath string, tlsKeyPath string) Option {
	return func(opts *options) {
		opts.tlsCertPath = tlsCertPath
		opts.tlsKeyPath = tlsKeyPath
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

func NewServer(addrPort string,  handler Handler, opts ...Option) (*Server, error) {
	baseOpts := defaultOptions()
	for _, opt := range opts {
		opt(baseOpts)
	}
	listen, err := net.Listen("tcp", addrPort)
	if err != nil {
		return nil, fmt.Errorf("failed to listen (addrPort = %v): %w", addrPort, err)
	}
	grpcServer := grpc.NewServer()
	if baseOpts.tlsCertPath != "" && baseOpts.tlsKeyPath != "" {
		serverCred, err := credentials.NewServerTLSFromFile(baseOpts.tlsCertPath, baseOpts.tlsKeyPath)
		if err != nil {
			return nil, fmt.Errorf("can not create server credential (tlsCertPath, tlsKeyPath = %v, $v): %w", baseOpts.tlsCertPath, baseOpts.tlsKeyPath, err)
		}
		grpcServer = grpc.NewServer(grpc.Creds(serverCred))
	}
	handler.Register(grpcServer)
	newServer := &Server{
		verbose: baseOpts.verbose,
		listen: listen,
		grpcServer: grpcServer,
		handler: handler,
	}
	return newServer, nil
}
