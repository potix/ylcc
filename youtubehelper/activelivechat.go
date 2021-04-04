package youtubehelper

import (
        "fmt"
        "time"
        "context"
        "google.golang.org/api/option"
	"google.golang.org/api/transport/http"
        "google.golang.org/api/youtube/v3"
)

type ActiveLiveChatParams struct {
	videoId          string
	activeLiveChatId string
	pageToken        string
}

func (a *ActiveLiveChatParams) GetPageToken() (string) {
	return a.pageToken
}

type ActiveLiveChatCollector struct {
	verbose bool
	apiKey  string
}

func (a *ActiveLiveChatCollector) CreateYoutubeService() (*youtube.Service, error) {
        ctx := context.Background()
        newClient, _, err := http.NewClient(ctx, option.WithAPIKey(a.apiKey))
        if err != nil {
		return nil, fmt.Errorf("can not create http clinet: %w", err)
        }
        youtubeService, err := youtube.New(newClient)
        if err != nil {
		return nil, fmt.Errorf("can not create youtube service: %w", err)
        }
	return youtubeService, nil
}

func (a *ActiveLiveChatCollector) GetVideo(videoId string, youtubeService *youtube.Service) (*youtube.Video, bool, error) {
        videosListCall := youtubeService.Videos.List([]string{"snippet", "contentDetails", "liveStreamingDetails", "status"})
        videosListCall.Id(videoId)
        videoListResponse, err := videosListCall.Do()
        if err != nil {
                return nil, false, fmt.Errorf("can not get videos (videoId = %v): %w", videoId, err)
        }
        if len(videoListResponse.Items) < 1 {
                return nil, false, nil
        }
	return videoListResponse.Items[0], true, nil
}

func (a *ActiveLiveChatCollector) CreateParams(video *youtube.Video) (*ActiveLiveChatParams, error) {
	if video.LiveStreamingDetails.ActiveLiveChatId == "" {
		return nil, fmt.Errorf("not active live chat (videoId = %v)", video.Id)
	}
	return &ActiveLiveChatParams {
		videoId: video.Id,
		activeLiveChatId: video.LiveStreamingDetails.ActiveLiveChatId,
		pageToken: "",
	}, nil
}

func (a *ActiveLiveChatCollector) GetActiveLiveChat(params *ActiveLiveChatParams, youtubeService *youtube.Service, max int64) (*youtube.LiveChatMessageListResponse, error) {
        liveChatMessagesListCall := youtubeService.LiveChatMessages.List(params.activeLiveChatId, []string{"snippet", "authorDetails"})
        liveChatMessagesListCall.PageToken(params.pageToken)
	liveChatMessagesListCall.MaxResults(max)
	liveChatMessageListResponse, err := liveChatMessagesListCall.Do()
	if err != nil {
		return nil, fmt.Errorf("can not get live chat messages (videoId = %v, activeLiveChatId = %v): %w", params.videoId, params.activeLiveChatId, err)
	}
	return liveChatMessageListResponse, nil
}

func (a *ActiveLiveChatCollector) Next(params *ActiveLiveChatParams, liveChatMessageListResponse *youtube.LiveChatMessageListResponse) (bool) {
	if liveChatMessageListResponse.NextPageToken == "" {
		return false
	}
	params.pageToken = liveChatMessageListResponse.NextPageToken
        time.Sleep(time.Duration(liveChatMessageListResponse.PollingIntervalMillis) * time.Millisecond)
	return true
}

func NewActiveLiveChatCollector(verbose bool, apiKey string) (*ActiveLiveChatCollector) {
	return &ActiveLiveChatCollector {
		verbose: verbose,
		apiKey: apiKey,
	}
}
