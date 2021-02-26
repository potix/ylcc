package server

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

// Get video information
func (h *handler) GetVideoInfo(ctx context.Context, request *GetVideoInfoRequst) (*GetVideoInfoResponse, error) {

}

// Start collect live chat
func (h *handler) StartCollectLiveChat(ctx context.Context, request *StartCollectLiveChatRequest) (*StartCollectLiveChatResponse, error) {

}

// Stop collect live chat
func (h *handler) StopCollectLiveChat(ctx context.Context, request *StopCollectLiveChatRequest) (*StopCollectLiveChatResponse, error) {

}

// Get recently live chat of video
func (s *server) GetRecentlyLiveChat(ctx context.Context, request *GetRecentlyLiveChatRequest) (*GetRecentlyLiveChatResponse, error) {

}

// Get all live chat of video
func (s *server) GetCompressedAllLiveChat(ctx context.Context, request *GetCompressedAllLiveChatRequest) (*GetCompressedAllLiveChatResponse, error) {

}

func NewHandler(verbose bool, collecot *collector) (Handler) {
	return &handler {
		verbose: verbose,
		collector: collector,
	}
}
