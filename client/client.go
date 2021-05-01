package main

import (
	"fmt"
	"io"
	"context"
	pb "github.com/potix/ylcc/protocol"
	"google.golang.org/grpc"
)

type YlccClinet struct {
	addrPort string
	options  []grpc.DialOption
	conn     *grpc.ClientConn
	client   pb.YlccClient
}

func (y *YlccClinet) Dial() (error) {
	conn, err := grpc.Dial(
		y.addrPort,
		y.options...,
	)
	if err != nil {
		return fmt.Errorf("can not dial")
	}
	y.conn = conn
	y.client = pb.NewYlccClient(conn)
	return nil
}

func (y *YlccClinet) GetVideo(ctx context.Context, videoId string) (*pb.GetVideoResponse, error) {
	request := &pb.GetVideoRequest{
		VideoId: videoId,
	}
	response, err := y.client.GetVideo(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get video: %w", err)
	}
	return response, err
}

func (y *YlccClinet) StartCollectionActiveLiveChat(ctx context.Context, videoId string) (*pb.StartCollectionActiveLiveChatResponse, error) {
	request := &pb.StartCollectionActiveLiveChatRequest{
		VideoId: videoId,
	}
	response, err := y.client.StartCollectionActiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start collection of active live chat: %w", err)
	}
	return response, err
}

func (y *YlccClinet) PollActiveLiveChat(ctx context.Context, videoId string, cbFunc func(*pb.PollActiveLiveChatResponse) (bool)) (error) {
	request := &pb.PollActiveLiveChatRequest{
		VideoId: videoId,
	}
	pollClient, err := y.client.PollActiveLiveChat(ctx, request)
	if err != nil {
		return fmt.Errorf("can not create stream clinet of active live chat: %w", err)
	}
	for {
		response, err := pollClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("can not recieve stream of active live chat: %w", err)
		}
		if cbFunc(response) {
			break
		}
	}
	return nil
}

func (y *YlccClinet) GetCachedActiveLiveChat(ctx context.Context, videoId string, offset int64, count int64) (*pb.GetCachedActiveLiveChatResponse, error) {
	request := &pb.GetCachedActiveLiveChatRequest{
		VideoId: videoId,
		Offset:  offset,
		Count:   count,
	}
	response, err := y.client.GetCachedActiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get cache of active live chat: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) StartCollectionArchiveLiveChat(ctx context.Context, videoId string) (*pb.StartCollectionArchiveLiveChatResponse, error) {
	request := &pb.StartCollectionArchiveLiveChatRequest{
		VideoId: videoId,
	}
	response, err := y.client.StartCollectionArchiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start collection of archive live chat: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) GetArchiveLiveChat(ctx context.Context, videoId string, offset int64, count int64) (*pb.GetArchiveLiveChatResponse, error) {
	request := &pb.GetArchiveLiveChatRequest{
		VideoId: videoId,
		Offset:  offset,
		Count:   count,
	}
	response, err := y.client.GetArchiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get archive live chat: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) StartCollectionWordCloudMessages(ctx context.Context, videoId string) (*pb.StartCollectionWordCloudMessagesResponse, error) {
	request := &pb.StartCollectionWordCloudMessagesRequest{
		VideoId: videoId,
	}
	response, err := y.client.StartCollectionWordCloudMessages(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start collection of word cloud message: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) BuildRGBAColor(r uint32, g uint32, b uint32, a uint32) (*pb.Color) {
	return &pb.Color{ R: r, G: g, B: b, A: a }
}

func (y *YlccClinet) BuildRGBColor(r uint32, g uint32, b uint32) (*pb.Color) {
	return &pb.Color{ R: r, G: g, B: b, A: 255 }
}

func (y *YlccClinet) GetWordCloud(
	ctx context.Context,
	videoId string,
	target pb.Target,
	messageLimit int32,
	fontMaxSize int32,
	fontMinSize int32,
	width int32,
	height int32,
	colors []*pb.Color,
	backgroundColor *pb.Color) (*pb.GetWordCloudResponse, error) {
	request := &pb.GetWordCloudRequest{
		VideoId: videoId,
		Target: target,
		MessageLimit: messageLimit,
		FontMaxSize: fontMaxSize,
		FontMinSize: fontMinSize,
		Width: width,
		Height: height,
		Colors: colors,
		BackgroundColor: backgroundColor,
	}
	response, err := y.client.GetWordCloud(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get word cloud: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) OpenVote(ctx context.Context, videoId string, target pb.Target, duration int32, choices []*pb.VoteChoice) (*pb.OpenVoteResponse, error) {
	request := &pb.OpenVoteRequest{
		VideoId: videoId,
		Target: target,
		Duration: duration,
		Choices: choices,
	}
	response, err := y.client.OpenVote(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not open vote: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) GetVoteResult(ctx context.Context, voteId string) (*pb.GetVoteResultResponse, error) {
	request := &pb.GetVoteResultRequest {
		VoteId: voteId,
	}
	response, err := y.client.GetVoteResult(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get vote result: %w", err)
	}
	return response, nil
}

func (y *YlccClinet) UpdateVoteDuration(ctx context.Context, voteId string, duration int32) (*pb.UpdateVoteDurationResponse, error) {
	request := &pb.UpdateVoteDurationRequest{
		VoteId: voteId,
		Duration: duration,
	}
	response, err := y.client.UpdateVoteDuration(ctx, request)
	if err != nil {
		return nil,fmt.Errorf("can not update duration", err)
	}
	return response, nil
}

func (y *YlccClinet) CloseVote(ctx context.Context, voteId string) (*pb.CloseVoteResponse, error) {
	request := &pb.CloseVoteRequest{
		VoteId: voteId,
	}
	response, err := y.client.CloseVote(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not close vote", err)
	}
	return response, nil
}

func NewYlccClinet(addrPort string, options ...grpc.DialOption) (*YlccClinet) {
	return &YlccClinet{
		addrPort: addrPort,
		options: options,
		conn: nil,
		client: nil,
	}
}
