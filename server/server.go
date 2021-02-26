package server

import (
        "log"
        "context"
        "time"
        "net/http"
        "github.com/gin-gonic/gin"
)

// Server is server
type Server struct {
	addrPort        string
        tlsCertPath     string
        tlsKeyPath      string
	release         bool
	verbose         bool
	shutdownTimeout int
        server          *http.Server
        router          *gin.Engine
	handler         *Handler
}

//Handler is server handler
type Handler interface {
        SetRouting(*gin.Engine)
        Start()
        Stop()
}

func (s *server) Start() {
	s.handler.Start()
        go func() {
		if s.tlsCertPath != "" && s.tlsKeyPath != "" {
			err := s.server.ListenAndServeTLS(s.tlsCertPath, s.tlsKeyPath);
			if err != nil && err != http.ErrServerClosed {
				log.Fatalf("listen: %v", err)
			}
		} else {
			err := s.server.ListenAndServe();
			if err != nil && err != http.ErrServerClosed {
				log.Fatalf("listen: %v", err)
			}
		}
        }()
}

func (s *server) Stop() {
        ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.shutdownTimeout) * time.Second)
        defer cancel()
        err := s.server.Shutdown(ctx)
        if err != nil {
            log.Printf("Server Shutdown: %v", err)
        }
	s.handler.Stop()
}

// NewServer is http server with tls 
func NewServer(addrPort string, tlsCertPath string, tlsKeyPath string, release bool, verbose bool, idleTimeout int, shutdownTimeout int, handler Handler) (*server) {
	if release {
		gin.SetMode(gin.ReleaseMode)
	}
        router := gin.Default()
	handler.SetRouting(router)
        s := &http.Server{
            Addr: addrPort,
            Handler: router,
            IdleTimeout: time.Duration(idleTimeout) * time.Second,
        }
        return &server {
		addrPort: addrPort,
		tlsCertPath: tlsCertPath,
		tlsKeyPath: tlsKeyPath,
		release: release,
		verbose: verbose,
		shutdownTimeout: shutdownTimeout,
		server: s,
		router: router,
		handler: handler,
        }
}
