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

func (h *handler) GetVideo(ctx context.Context, request *pb.GetVideoRequest, opts ...grpc.CallOption) (*pb.GetVideoResponse, error) {
	video, err := g.controller.getVideo(request.VideoId)
	return &pb.GetVideoResponse{
		Status : &pb.Status{
			Code: Code_SUCCESS,
			Mesage: "success"
		}
		Video : &pb.Video{
			VideoId: video.VideoId,
			ChannelId: video.channelId
			CategoryId: video.CategoryId,
			Title:
			Description:
			PublishedAt:
			Duration:
			ActiveLiveChatId:
			ActualStartTime:
			ActualEndTime:
			ScheduledStartTime:
			ScheduledEndTime:
			PrivacyStatus:
			UploadStatus:
			Embeddable:
		}
	}, nil
}

func (h *handler) StartCollectionActiveLiveChat(ctx context.Context, request *pb.StartCollectionActiveLiveChatRequest, opts ...grpc.CallOption) (*pb.StartCollectionActiveLiveChatResponse, error) {
	video, err := g.controller.startCollectionActiveLiveChat(request.VideoId)
	return &pb.GetVideoResponse{
		Status : &pb.Status{
			Code:
			Mesage:
		}
		Video : &pb.Video{
			VideoId:
			ChannelId:
			CategoryId:
			Title:
			Description:
			PublishedAt:
			Duration:
			ActiveLiveChatId:
			ActualStartTime:
			ActualEndTime:
			ScheduledStartTime:
			ScheduledEndTime:
			PrivacyStatus:
			UploadStatus:
			Embeddable:
		}
	}, nil

}

func (h *handler) PollActiveLiveChat(ctx context.Context, request *pb.PollActiveLiveChatRequest, opts ...grpc.CallOption) (pb.Ylcc_PollActiveLiveChatClient, error) {


}

func (h *handler) GetCachedActiveLiveChat(ctx context.Context, request *pb.GetCachedActiveLiveChatRequest, opts ...grpc.CallOption) (*pb.GetCachedActiveLiveChatResponse, error) {

}

func (h *handler) StartCollectionArchiveLiveChat(ctx context.Context, request *pb.StartCollectionArchiveLiveChatRequest, opts ...grpc.CallOption) (*pb.StartCollectionArchiveLiveChatResponse, error) {

}

func (h *handler) GetArchiveLiveChat(ctx context.Context, request *pb.GetArchiveLiveChatRequest, opts ...grpc.CallOption) (*pb.GetArchiveLiveChatResponse, error) {

}




func NewHandler(verbose bool, collecot *collector) (Handler) {
	return &handler {
		verbose: verbose,
		collector: collector,
	}
}
