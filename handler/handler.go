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

// StartMonitor video
func (h *handler) StartMonitorVideo(ctx context.Context, request *StartMonitorVideoRequest) (*StartMonitorVideoResponse, error) {

}

// StopMonitor video
func (h *handler) StopMonitorVideo(ctx context.Context, request *StopMonitorVideoRequest) (*StopMonitorVideoResponse, error) {

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
