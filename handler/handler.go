package server

import (
	"google.golang.org/grpc"
	pb "github.com/potix/ylcc/protocol"
)

type handler struct {
	verbose   bool
	collector *collector
}

// Start
func (h *handler) Start() {
	h.collector.Start()
}

// Stop
func (h *handler) Stop() {
	h.collector.Stop()
}

// register
func (h *handler) Register(grpcServer *grpc.Server) {
	pb.RegisterYlccServer(grpcServer, handler)
}



// Get video information
func (h *handler) GetVideoInfo(ctx context.Context, request *pb.GetVideoInfoRequst) (*pb.GetVideoInfoResponse, error) {

}

// Start collect live chat
func (h *handler) StartCollectLiveChat(ctx context.Context, request *pb.StartCollectLiveChatRequest) (*pb.StartCollectLiveChatResponse, error) {

}

// delete live chat
func (h *handler) DeleteLiveChat(ctx context.Context, request *pb.StopCollectLiveChatRequest) (*pb.DeleteLiveChatResponse, error) {

}

// Get recently live chat of video
func (s *server) GetRecentlyActiveLiveChat(ctx context.Context, request *pb.GetRecentlyLiveChatRequest) (*pb.GetRecentlyActiveLiveChatResponse, error) {

}

// Get all live chat of video
func (s *server) GetCompressedLiveChat(ctx context.Context, request *pb.GetCompressedLiveChatRequest) (*pb.GetCompressedLiveChatResponse, error) {

}



func NewHandler(verbose bool, collecot *collector) (Handler) {
	return &handler {
		verbose: verbose,
		collector: collector,
	}
}
