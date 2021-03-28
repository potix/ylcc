package collector

import (
	"fmt"
	"context"
	"google.golang.org/grpc"
	pb "github.com/potix/ylcc/protocol"
)

type Handler struct {
	verbose   bool
	collector *Collector
	pb.UnimplementedYlccServer
}

func (h *Handler) Start() (error) {
	if err := h.collector.start(); err != nil {
		return fmt.Errorf("can not start collector %w", err)
	}
	return nil
}

func (h *Handler) Stop() {
	h.collector.stop()
}

func (h *Handler) Register(grpcServer *grpc.Server) {
	pb.RegisterYlccServer(grpcServer, h)
}

func (h *Handler) GetVideo(ctx context.Context, request *pb.GetVideoRequest) (*pb.GetVideoResponse, error) {
	return h.collector.getVideo(request)
}

func (h *Handler) StartCollectionActiveLiveChat(ctx context.Context, request *pb.StartCollectionActiveLiveChatRequest) (*pb.StartCollectionActiveLiveChatResponse, error) {
	return h.collector.startCollectionActiveLiveChat(request)
}

func (h *Handler) PollActiveLiveChat(request *pb.PollActiveLiveChatRequest, server pb.Ylcc_PollActiveLiveChatServer) (error) {
	subscribeActiveLiveChatParams := h.collector.subscribeActiveLiveChat(request)
	defer h.collector.unsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
	for {
		response, ok := <-subscribeActiveLiveChatParams.subscriberCh
		if !ok {
			return nil
		}
		if err := server.Send(response); err != nil {
			return fmt.Errorf("can not send response: %w", err)
		}
	}
}

func (h *Handler) GetCachedActiveLiveChat(ctx context.Context, request *pb.GetCachedActiveLiveChatRequest) (*pb.GetCachedActiveLiveChatResponse, error) {
	return h.collector.getCachedActiveLiveChat(request)
}

func (h *Handler) StartCollectionArchiveLiveChat(ctx context.Context, request *pb.StartCollectionArchiveLiveChatRequest) (*pb.StartCollectionArchiveLiveChatResponse, error) {
	return h.collector.startCollectionArchiveLiveChat(request)
}

func (h *Handler) GetArchiveLiveChat(ctx context.Context, request *pb.GetArchiveLiveChatRequest) (*pb.GetArchiveLiveChatResponse, error) {
	return h.collector.getArchiveLiveChat(request)
}

func NewHandler(verbose bool, collector *Collector) (*Handler) {
	return &Handler {
		verbose: verbose,
		collector: collector,
	}
}
