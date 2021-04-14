package handler

import (
	"context"
	"fmt"
	"github.com/potix/ylcc/collector"
	"github.com/potix/ylcc/processor"
	pb "github.com/potix/ylcc/protocol"
	"google.golang.org/grpc"
)

type options struct {
	verbose bool
}

func defaultOptions() *options {
	return &options{
		verbose: false,
	}
}

type Option func(*options)

func Verbose(verbose bool) Option {
	return func(opts *options) {
		opts.verbose = verbose
	}
}

type Handler struct {
	verbose   bool
	processor *processor.Processor
	collector *collector.Collector
	pb.UnimplementedYlccServer
}

func (h *Handler) Start() error {
	if err := h.collector.Start(); err != nil {
		return fmt.Errorf("can not start collector %w", err)
	}
	return nil
}

func (h *Handler) Stop() {
	h.collector.Stop()
}

func (h *Handler) Register(grpcServer *grpc.Server) {
	pb.RegisterYlccServer(grpcServer, h)
}

func (h *Handler) GetVideo(ctx context.Context, request *pb.GetVideoRequest) (*pb.GetVideoResponse, error) {
	return h.collector.GetVideo(request)
}

func (h *Handler) StartCollectionActiveLiveChat(ctx context.Context, request *pb.StartCollectionActiveLiveChatRequest) (*pb.StartCollectionActiveLiveChatResponse, error) {
	return h.collector.StartCollectionActiveLiveChat(request)
}

func (h *Handler) PollActiveLiveChat(request *pb.PollActiveLiveChatRequest, server pb.Ylcc_PollActiveLiveChatServer) error {
	subscribeActiveLiveChatParams := h.collector.SubscribeActiveLiveChat(request.VideoId)
	defer h.collector.UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
	for {
		response, ok := <-subscribeActiveLiveChatParams.GetSubscriberCh()
		if !ok {
			return nil
		}
		if err := server.Send(response); err != nil {
			return fmt.Errorf("can not send response: %w", err)
		}
	}
}

func (h *Handler) GetCachedActiveLiveChat(ctx context.Context, request *pb.GetCachedActiveLiveChatRequest) (*pb.GetCachedActiveLiveChatResponse, error) {
	return h.collector.GetCachedActiveLiveChat(request)
}

func (h *Handler) StartCollectionArchiveLiveChat(ctx context.Context, request *pb.StartCollectionArchiveLiveChatRequest) (*pb.StartCollectionArchiveLiveChatResponse, error) {
	return h.collector.StartCollectionArchiveLiveChat(request)
}

func (h *Handler) GetArchiveLiveChat(ctx context.Context, request *pb.GetArchiveLiveChatRequest) (*pb.GetArchiveLiveChatResponse, error) {
	return h.collector.GetArchiveLiveChat(request)
}

func (h *Handler) StartCollectionWordCloudMessages(ctx context.Context, request *pb.StartCollectionWordCloudMessagesRequest) (*pb.StartCollectionWordCloudMessagesResponse, error) {
	return h.processor.StartCollectionWordCloudMessages(request)
}

func (h *Handler) GetWordCloud(ctx context.Context, request *pb.GetWordCloudRequest) (*pb.GetWordCloudResponse, error) {
	return h.processor.GetWordCloud(request)
}

func (h *Handler) OpenVote(ctx context.Context, request *pb.OpenVoteRequest)  (*pb.OpenVoteResponse, error) {
	return h.processor.OpenVote(request)
}

func (h *Handler) UpdateVoteDuration(ctx context.Context, request *pb.UpdateVoteDurationRequest) (*pb.UpdateVoteDurationResponse, error) {
	return h.processor.UpdateVoteDuration(request)
}

func (h *Handler) GetVoteResut(ctx context.Context, request *pb.GetVoteResultRequest) (*pb.GetVoteResultResponse, error) {
	return h.processor.GetVoteResut(request)
}

func (h *Handler) CloseVote(ctx context.Context, request *pb.CloseVoteRequest) (*pb.CloseVoteResponse, error) {
	return h.processor.CloseVote(request)
}

func NewHandler(processor *processor.Processor, collector *collector.Collector, opts ...Option) *Handler {
	baseOpts := &options{
		verbose: false,
	}
	for _, opt := range opts {
		opt(baseOpts)
	}
	return &Handler{
		verbose:   baseOpts.verbose,
		processor: processor,
		collector: collector,
	}
}
