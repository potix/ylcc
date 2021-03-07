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
	err := h.collector.Start()
	if err != nil {
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
	return h.collector.GetVideo(request)
}

func (h *handler) StartCollectionActiveLiveChat(ctx context.Context, request *StartCollectionActiveLiveChatRequest) (*StartCollectionActiveLiveChatResponse, error) {
	return h.collector.StartCollectionActiveLiveChat(request)
}

func (h *handler) PollActiveLiveChat(request *PollActiveLiveChatRequest, server Ylcc_PollActiveLiveChatServer) (error) {
	myCh := make(chan *PollActiveLiveChatResponse)
	h.collector.PollActiveLiveChatSubscribe(myCh, request)
	defer h.collector.PollActiveLiveChatUnsubscribe(myCh)
	for {
		select {
		case response <-myCh:
			err := server.Send(response)
			if err != nil {
				return fmt.Errorf("can not send response: %w", err)
			}
		default:
			return fmt.Errorf("can not read channnel. probably closed channel.")
		}
	}
}

func (h *handler) GetCachedActiveLiveChat(ctx context.Context, server *GetCachedActiveLiveChatRequest) (*GetCachedActiveLiveChatResponse, error) {
	return h.collector.GetCachedActiveLiveChat(request)
}

func (h *handler) StartCollectionArchiveLiveChat(ctx context.Context, request *StartCollectionArchiveLiveChatRequest) (*StartCollectionArchiveLiveChatResponse, error) {
	return h.collector.StartCollectionArchiveLiveChat(request)
}

func (h *handler) GetArchiveLiveChat(ctx context.Context, request *GetArchiveLiveChatRequest) (*GetArchiveLiveChatResponse, error) {
	return h.collector.GetArchiveLiveChat(request)
}

func NewHandler(verbose bool, collecot *collector) (Handler) {
	return &handler {
		verbose: verbose,
		collector: collector,
	}
}
