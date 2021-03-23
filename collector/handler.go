package collector

import (
	"google.golang.org/grpc"
	pb "github.com/potix/ylcc/protocol"
)

type handler struct {
	verbose   bool
	collector *collector
}

func (h *handler) Start() (error) {
	if err := h.collector.Start(); err != nil {
		return fmt.Errorf("can not start collector %w", err)
	}
	return nil
}

func (h *handler) Stop() {
	h.collector.Stop()
}

func (h *handler) Register(grpcServer *grpc.Server) {
	pb.RegisterYlccServer(grpcServer, handler)
}

func (h *handler) GetVideo(ctx context.Context, request *GetVideoRequest) (*GetVideoResponse, error) {
	return h.collector.getVideo(request)
}

func (h *handler) StartCollectionActiveLiveChat(ctx context.Context, request *StartCollectionActiveLiveChatRequest) (*StartCollectionActiveLiveChatResponse, error) {
	return h.collector.startCollectionActiveLiveChat(request)
}

func (h *handler) PollActiveLiveChat(request *PollActiveLiveChatRequest, server Ylcc_PollActiveLiveChatServer) (error) {
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

func (h *handler) GetCachedActiveLiveChat(ctx context.Context, request *GetCachedActiveLiveChatRequest) (*GetCachedActiveLiveChatResponse, error) {
	return h.collector.getCachedActiveLiveChat(request)
}

func (h *handler) StartCollectionArchiveLiveChat(ctx context.Context, request *StartCollectionArchiveLiveChatRequest) (*StartCollectionArchiveLiveChatResponse, error) {
	return h.collector.startCollectionArchiveLiveChat(request)
}

func (h *handler) GetArchiveLiveChat(ctx context.Context, request *GetArchiveLiveChatRequest) (*GetArchiveLiveChatResponse, error) {
	return h.collector.getArchiveLiveChat(request)
}

func NewHandler(verbose bool, collecot *collector) (Handler) {
	return &handler {
		verbose: verbose,
		collector: collector,
	}
}
