package client

import (
	"fmt"
	"io"
	"context"
	pb "github.com/potix/ylcc/protocol"
	"google.golang.org/grpc"
)

type YlccClient struct {
	addrPort string
	options  []grpc.DialOption
	conn     *grpc.ClientConn
	client   pb.YlccClient
}

func (y *YlccClient) Dial() (error) {
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

func (y *YlccClient) Close() {
	y.conn.Close()
}

func (y *YlccClient) GetVideo(ctx context.Context, videoId string) (*pb.GetVideoResponse, error) {
	request := &pb.GetVideoRequest{
		VideoId: videoId,
	}
	response, err := y.client.GetVideo(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get video: %w", err)
	}
	return response, err
}

func (y *YlccClient) StartCollectionActiveLiveChat(ctx context.Context, videoId string) (*pb.StartCollectionActiveLiveChatResponse, error) {
	request := &pb.StartCollectionActiveLiveChatRequest{
		VideoId: videoId,
	}
	response, err := y.client.StartCollectionActiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start collection of active live chat: %w", err)
	}
	return response, err
}

func (y *YlccClient) PollActiveLiveChat(ctx context.Context, videoId string, cbFunc func(*pb.PollActiveLiveChatResponse) (bool)) (error) {
	request := &pb.PollActiveLiveChatRequest{
		VideoId: videoId,
	}
	pollClient, err := y.client.PollActiveLiveChat(ctx, request)
	if err != nil {
		return fmt.Errorf("can not create stream client of active live chat: %w", err)
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

func (y *YlccClient) GetCachedActiveLiveChat(ctx context.Context, videoId string, offset int64, count int64) (*pb.GetCachedActiveLiveChatResponse, error) {
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

func (y *YlccClient) StartCollectionArchiveLiveChat(ctx context.Context, videoId string) (*pb.StartCollectionArchiveLiveChatResponse, error) {
	request := &pb.StartCollectionArchiveLiveChatRequest{
		VideoId: videoId,
	}
	response, err := y.client.StartCollectionArchiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start collection of archive live chat: %w", err)
	}
	return response, nil
}

func (y *YlccClient) GetArchiveLiveChat(ctx context.Context, videoId string, offset int64, count int64) (*pb.GetArchiveLiveChatResponse, error) {
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

func (y *YlccClient) StartCollectionWordCloudMessages(ctx context.Context, videoId string) (*pb.StartCollectionWordCloudMessagesResponse, error) {
	request := &pb.StartCollectionWordCloudMessagesRequest{
		VideoId: videoId,
	}
	response, err := y.client.StartCollectionWordCloudMessages(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start collection of word cloud message: %w", err)
	}
	return response, nil
}

func (y *YlccClient) BuildRGBAColor(r uint32, g uint32, b uint32, a uint32) (*pb.Color) {
	return &pb.Color{ R: r, G: g, B: b, A: a }
}

func (y *YlccClient) BuildRGBColor(r uint32, g uint32, b uint32) (*pb.Color) {
	return &pb.Color{ R: r, G: g, B: b, A: 255 }
}

func (y *YlccClient) GetWordCloud(
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

func (y *YlccClient) BuildVoteChoice(label string, choice string) (*pb.VoteChoice) {
	return &pb.VoteChoice {
		Label: label,
		Choice: choice,
	}
}

func (y *YlccClient) OpenVote(ctx context.Context, videoId string, target pb.Target, duration int32, choices []*pb.VoteChoice) (*pb.OpenVoteResponse, error) {
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

func (y *YlccClient) GetVoteResult(ctx context.Context, voteId string) (*pb.GetVoteResultResponse, error) {
	request := &pb.GetVoteResultRequest {
		VoteId: voteId,
	}
	response, err := y.client.GetVoteResult(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not get vote result: %w", err)
	}
	return response, nil
}

func (y *YlccClient) UpdateVoteDuration(ctx context.Context, voteId string, duration int32) (*pb.UpdateVoteDurationResponse, error) {
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

func (y *YlccClient) CloseVote(ctx context.Context, voteId string) (*pb.CloseVoteResponse, error) {
	request := &pb.CloseVoteRequest{
		VoteId: voteId,
	}
	response, err := y.client.CloseVote(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not close vote", err)
	}
	return response, nil
}

func (y *YlccClient) BuildGroupingChoice(label string, choice string) (*pb.GroupingChoice) {
	return &pb.GroupingChoice {
		Label: label,
		Choice: choice,
	}
}

func (y *YlccClient) StartGroupingActiveLiveChat(ctx context.Context, videoId string, target pb.Target, choices []*pb.GroupingChoice) (*pb.StartGroupingActiveLiveChatResponse, error) {
	request := &pb.StartGroupingActiveLiveChatRequest{
		VideoId: videoId,
		Target: target,
		Choices: choices,
	}
	response, err := y.client.StartGroupingActiveLiveChat(ctx, request)
	if err != nil {
		return nil, fmt.Errorf("can not start grouping: %w", err)
	}
	return response, nil
}

func (y *YlccClient) PollGroupingActiveLiveChat(ctx context.Context, groupingId string, cbFunc func(*pb.PollGroupingActiveLiveChatResponse) (bool)) (error) {
	request := &pb.PollGroupingActiveLiveChatRequest{
		GroupingId: groupingId,
	}
	pollClient, err := y.client.PollGroupingActiveLiveChat(ctx, request)
	if err != nil {
		return fmt.Errorf("can not create stream client of active live chat: %w", err)
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


func NewYlccClient(addrPort string, options ...grpc.DialOption) (*YlccClient) {
	return &YlccClient{
		addrPort: addrPort,
		options: options,
		conn: nil,
		client: nil,
	}
}
