package collector

import (
        "fmt"
        "log"
        "sync"
        "time"
        "context"
        "google.golang.org/api/option"
	"google.golang.org/api/transport/http"
        "google.golang.org/api/youtube/v3"
	 pb "github.com/potix/ylcc/protocol"
)

const (
	messageMax int64 = 2000
	retryMax int = 10
)

type Collector struct {
	verbose                               bool
	apiKey                                string
	dbOperator                            *DatabaseOperator
	requestedVideoForActiveLiveChatMutex  *sync.Mutex
	requestedVideoForActiveLiveChat	      map[string]bool
	requestedVideoForArchiveLiveChatMutex *sync.Mutex
	requestedVideoForArchiveLiveChat      map[string]bool
	publishActiveLiveChatCh               chan *publishActiveLiveChatMessagesParams
	subscribeActiveLiveChatCh             chan *subscribeActiveLiveChatParams
	unsubscribeActiveLiveChatCh           chan *subscribeActiveLiveChatParams
	publisherFinishRequestCh              chan int
	publisherFinishResponseCh             chan int
}

type publishActiveLiveChatMessagesParams struct {
	err                    error
	videoId                string
	activeLiveChatMessages []*pb.ActiveLiveChatMessage
}

type subscribeActiveLiveChatParams struct {
	videoId      string
	subscriberCh chan *pb.PollActiveLiveChatResponse
}

func (c *Collector) registerRequestedVideoForActiveLiveChat(videoId string) (bool) {
	c.requestedVideoForActiveLiveChatMutex.Lock()
	defer c.requestedVideoForActiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForActiveLiveChat[videoId]
	if ok {
		return false
	}
	c.requestedVideoForActiveLiveChat[videoId] = true
	return true
}

func (c *Collector) checkRequestedVideoForActiveLiveChat(videoId string) (bool) {
	c.requestedVideoForActiveLiveChatMutex.Lock()
	defer c.requestedVideoForActiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForActiveLiveChat[videoId]
	if ok {
		return true
	}
	return false
}

func (c *Collector) unregisterRequestedVideoForActiveLiveChat(videoId string) (bool) {
	c.requestedVideoForActiveLiveChatMutex.Lock()
	defer c.requestedVideoForActiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForActiveLiveChat[videoId]
	if !ok {
		return false
	}
	delete(c.requestedVideoForActiveLiveChat, videoId)
	return true
}


func (c *Collector) registerRequestedVideoForArchiveLiveChat(videoId string) (bool) {
	c.requestedVideoForArchiveLiveChatMutex.Lock()
	defer c.requestedVideoForArchiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForArchiveLiveChat[videoId]
	if ok {
		return false
	}
	c.requestedVideoForArchiveLiveChat[videoId] = true
	return true
}

func (c *Collector) checkRequestedVideoForArchiveLiveChat(videoId string) (bool) {
	c.requestedVideoForArchiveLiveChatMutex.Lock()
	defer c.requestedVideoForArchiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForArchiveLiveChat[videoId]
	if ok {
		return true
	}
	return false
}

func (c *Collector) unregisterRequestedVideoForArchiveLiveChat(videoId string) (bool) {
	c.requestedVideoForArchiveLiveChatMutex.Lock()
	defer c.requestedVideoForArchiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForArchiveLiveChat[videoId]
	if !ok {
		return false
	}
	delete(c.requestedVideoForArchiveLiveChat, videoId)
	return true
}

func (c *Collector) createYoutubeService() (*youtube.Service, error) {
        ctx := context.Background()
        newClient, _, err := http.NewClient(ctx, option.WithAPIKey(c.apiKey))
        if err != nil {
		return nil, fmt.Errorf("can not create http clinet: %w", err)
        }
        youtubeService, err := youtube.New(newClient)
        if err != nil {
		return nil, fmt.Errorf("can not create youtube service: %w", err)
        }
	return youtubeService, nil
}

func (c *Collector) getVideoFromYoutube(videoId string, youtubeService *youtube.Service) (*pb.Video, bool, error) {
        videosListCall := youtubeService.Videos.List([]string{"snippet", "liveStreamingDetails"})
        videosListCall.Id(videoId)
        videoListResponse, err := videosListCall.Do()
        if err != nil {
                return nil, false, fmt.Errorf("can not get videos: %w", err)
        }
        if len(videoListResponse.Items) < 1 {
                return nil, true, nil
        }
	item := videoListResponse.Items[0]
        video := &pb.Video {
                VideoId: item.Id,
                ChannelId: item.Snippet.ChannelId,
                CategoryId: item.Snippet.CategoryId,
                Title: item.Snippet.Title,
                Description: item.Snippet.Description,
                PublishedAt: item.Snippet.PublishedAt,
                Duration: item.ContentDetails.Duration,
                ActiveLiveChatId: item.LiveStreamingDetails.ActiveLiveChatId,
                ActualStartTime: item.LiveStreamingDetails.ActualStartTime,
                ActualEndTime: item.LiveStreamingDetails.ActualEndTime,
                ScheduledStartTime: item.LiveStreamingDetails.ScheduledStartTime,
                ScheduledEndTime: item.LiveStreamingDetails.ScheduledEndTime,
                PrivacyStatus: item.Status.PrivacyStatus,
                UploadStatus: item.Status.UploadStatus,
                Embeddable : item.Status.Embeddable,
        }
        return video, true, nil
}

func (c *Collector) collectActiveLiveChatFromYoutube(channelId string, videoId string, youtubeService *youtube.Service, activeLiveChatId string) {
	pageToken := ""
        for {
                liveChatMessagesListCall := youtubeService.LiveChatMessages.List(activeLiveChatId, []string{"snippet", "authorDetails"})
                liveChatMessagesListCall.PageToken(pageToken)
                liveChatMessagesListCall.MaxResults(messageMax)
                liveChatMessageListResponse, err := liveChatMessagesListCall.Do()
                if err != nil {
			c.publishActiveLiveChatCh <-&publishActiveLiveChatMessagesParams {
				err: err,
				videoId: videoId,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(videoId)
			if c.verbose {
				log.Printf("can not get live chat messages: %v\n", err)
			}
                        return
                }
		nextPageToken := liveChatMessageListResponse.NextPageToken
                pollingIntervalMillis := liveChatMessageListResponse.PollingIntervalMillis
		activeLiveChatMessages := make([]*pb.ActiveLiveChatMessage, 0, messageMax)
                for _, item := range liveChatMessageListResponse.Items {
			displayMessage := ""
			amountDisplayString := ""
			currency := ""
			isSuperChat := false
                        if item.Snippet.SuperChatDetails != nil {
				displayMessage = item.Snippet.SuperChatDetails.UserComment
                                amountDisplayString = item.Snippet.SuperChatDetails.AmountDisplayString
                                currency = item.Snippet.SuperChatDetails.AmountDisplayString
				isSuperChat = true
                        } else if item.Snippet.TextMessageDetails != nil {
                                displayMessage = item.Snippet.TextMessageDetails.MessageText
                        }
			activeLiveChatMessage := &pb.ActiveLiveChatMessage{
				MessageId: item.Id,
				ChannelId: channelId,
				VideoId: videoId,
				ApiEtag: liveChatMessageListResponse.Etag,
				AuthorChannelId: item.AuthorDetails.ChannelId,
				AuthorChannelUrl: item.AuthorDetails.ChannelUrl,
				AuthorDisplayName: item.AuthorDetails.DisplayName,
				AuthorIsChatModerator: item.AuthorDetails.IsChatModerator,
				AuthorIsChatOwner: item.AuthorDetails.IsChatOwner,
				AuthorIsChatSponsor: item.AuthorDetails.IsChatSponsor,
				AuthorIsVerified: item.AuthorDetails.IsVerified,
				LiveChatId: item.Snippet.LiveChatId,
				DisplayMessage: displayMessage,
				PublishedAt: item.Snippet.PublishedAt,
				IsSuperChat: isSuperChat,
				AmountDisplayString: amountDisplayString,
				Currency: currency,
			}
			activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
                }
		if err := c.dbOperator.UpdateActiveLiveChatMessages(pageToken, nextPageToken, activeLiveChatMessages); err != nil {
			c.publishActiveLiveChatCh <- &publishActiveLiveChatMessagesParams {
				err: err,
				videoId: videoId,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(videoId)
			log.Printf("can not update active live chat messages in database: %v\n", err)
                        return
                }
		c.publishActiveLiveChatCh <- &publishActiveLiveChatMessagesParams {
			err: nil,
			videoId: videoId,
			activeLiveChatMessages: activeLiveChatMessages,
		}
                time.Sleep(time.Duration(pollingIntervalMillis) * time.Millisecond)
		pageToken = nextPageToken
        }
}

func (c *Collector) getVideo(request *pb.GetVideoRequest) (*pb.GetVideoResponse, error) {
	status := new(pb.Status)
	video, ok, err := c.dbOperator.GetVideoByVideoId(request.VideoId)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		return &pb.GetVideoResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
	if !ok {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		return &pb.GetVideoResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.GetVideoResponse {
		Status: status,
		Video: video,
	}, nil
}

func (c *Collector) startCollectionActiveLiveChat(request *pb.StartCollectionActiveLiveChatRequest) (*pb.StartCollectionActiveLiveChatResponse, error) {
	status := new(pb.Status)
	ok := c.registerRequestedVideoForActiveLiveChat(request.VideoId)
	if !ok {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	youtubeService, err := c.createYoutubeService()
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	video, ok, err := c.getVideoFromYoutube(request.VideoId, youtubeService)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
	if !ok {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
        err = c.dbOperator.UpdateVideo(video)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
	if video.ActiveLiveChatId == "" {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not live video (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
        }
	go c.collectActiveLiveChatFromYoutube(video.ChannelId, video.VideoId, youtubeService, video.ActiveLiveChatId)
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.StartCollectionActiveLiveChatResponse {
		Status: status,
		Video: video,
	}, nil
}

func (c *Collector) getCachedActiveLiveChat(request *pb.GetCachedActiveLiveChatRequest) (*pb.GetCachedActiveLiveChatResponse, error) {
	status := new(pb.Status)
	progress := c.checkRequestedVideoForActiveLiveChat(request.VideoId)
	if progress {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v,  = %v)", request.VideoId, request.PageToken)
		return &pb.GetCachedActiveLiveChatResponse {
			Status: status,
			NextPageToken: "",
			ActiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	nextPageToken, activeLiveChatMessages, err :=  c.dbOperator.GetActiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.PageToken)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v,  = %v)", err, request.VideoId, request.PageToken)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.GetCachedActiveLiveChatResponse {
			Status: status,
			NextPageToken: "",
			ActiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v,  = %v)", request.VideoId, request.PageToken)
	return &pb.GetCachedActiveLiveChatResponse {
		Status: status,
		NextPageToken: nextPageToken,
		ActiveLiveChatMessages: activeLiveChatMessages,
	}, nil
}

func  (c *Collector) startCollectionArchiveLiveChat(request *pb.StartCollectionArchiveLiveChatRequest) (*pb.StartCollectionArchiveLiveChatResponse, error) {
	status := new(pb.Status)
	ok := c.registerRequestedVideoForArchiveLiveChat(request.VideoId)
	if !ok {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	youtubeService, err := c.createYoutubeService()
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	video, ok, err := c.getVideoFromYoutube(request.VideoId, youtubeService)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
	if !ok {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
        err = c.dbOperator.UpdateVideo(video)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
	}
	if video.ActiveLiveChatId != "" {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not archive video(videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}, fmt.Errorf("%v", status.Message)
        }
	go c.collectArchiveLiveChatFromYoutube(video.ChannelId, video.VideoId, request.Replace)
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.StartCollectionArchiveLiveChatResponse {
		Status: status,
		Video: video,
	}, nil
}

func (c *Collector) getArchiveLiveChat(request *pb.GetArchiveLiveChatRequest) (*pb.GetArchiveLiveChatResponse, error) {
	status := new(pb.Status)
	progress := c.checkRequestedVideoForArchiveLiveChat(request.VideoId)
	if progress {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v,  = %v)", request.VideoId, request.PageToken)
		return &pb.GetArchiveLiveChatResponse {
			Status: status,
			NextPageToken: "",
			ArchiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	nextPageToken, archiveLiveChatMessages, err :=  c.dbOperator.GetArchiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.PageToken)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v,  = %v)", err, request.VideoId, request.PageToken)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.GetArchiveLiveChatResponse {
			Status: status,
			NextPageToken: "",
			ArchiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v,  = %v)", request.VideoId, request.PageToken)
	return &pb.GetArchiveLiveChatResponse {
		Status: status,
		NextPageToken: nextPageToken,
		ArchiveLiveChatMessages: archiveLiveChatMessages,
	}, nil
}

func (c *Collector) subscribeActiveLiveChat(request *pb.PollActiveLiveChatRequest) (*subscribeActiveLiveChatParams) {
        subscribeActiveLiveChatParams := &subscribeActiveLiveChatParams {
                videoId: request.VideoId,
                subscriberCh: make(chan *pb.PollActiveLiveChatResponse),
        }
	c.subscribeActiveLiveChatCh <-subscribeActiveLiveChatParams
	return subscribeActiveLiveChatParams
}

func (c *Collector) unsubscribeActiveLiveChat(subscribeActiveLiveChatParams *subscribeActiveLiveChatParams) {
	c.unsubscribeActiveLiveChatCh <-subscribeActiveLiveChatParams;
}

func (c *Collector) publisher() {
	activeLiveChatSubscribers := make(map[string]map[chan *pb.PollActiveLiveChatResponse]bool)
	for {
                select {
		case publishActiveLiveChatMessagesParams := <-c.publishActiveLiveChatCh:
			err := publishActiveLiveChatMessagesParams.err
			videoId := publishActiveLiveChatMessagesParams.videoId
			activeLiveChatMessages := publishActiveLiveChatMessagesParams.activeLiveChatMessages
			videoSubscribers, ok := activeLiveChatSubscribers[videoId]
			if !ok {
				if c.verbose {
					log.Printf("no subscribers for active live chat. no videoId. (videoId = %v)", videoId)
				}
				break
			}
			if err != nil {
				for subscriberCh, _ := range videoSubscribers {
					delete(videoSubscribers, subscriberCh)
					close(subscriberCh)
				}
				delete(activeLiveChatSubscribers, videoId)
				break
			}
			for subscriberCh, _ := range videoSubscribers {
				subscriberCh <-&pb.PollActiveLiveChatResponse {
					Status: &pb.Status{
						Code: pb.Code_SUCCESS,
						Message: fmt.Sprintf("success (vidoeId = %v)", videoId),
					},
					ActiveLiveChatMessages: activeLiveChatMessages,
				}
			}
		case subscribeActiveLiveChatParams := <-c.subscribeActiveLiveChatCh:
			videoId := subscribeActiveLiveChatParams.videoId
			subscriberCh := subscribeActiveLiveChatParams.subscriberCh
			videoSubscribers, ok := activeLiveChatSubscribers[videoId]
			if !ok {
				videoSubscribers = make(map[chan *pb.PollActiveLiveChatResponse]bool)
				activeLiveChatSubscribers[videoId] = videoSubscribers
			}
			videoSubscribers[subscriberCh] = true
		case subscribeActiveLiveChatParams := <-c.unsubscribeActiveLiveChatCh:
			videoId := subscribeActiveLiveChatParams.videoId
			subscriberCh := subscribeActiveLiveChatParams.subscriberCh
			videoSubscribers, ok := activeLiveChatSubscribers[videoId]
			if !ok {
				if c.verbose {
					log.Printf("no subscribers for active live chat. no videoId. (videoId = %v, subscriberCh = %v)", videoId, subscriberCh)
				}
				break
			}
			_, ok = videoSubscribers[subscriberCh]
			if !ok {
				if c.verbose {
					log.Printf("no subscriber for active live chat. no subscriber channel. (videoId = %v, subscriberCh = %v)", videoId, subscriberCh)
				}
				break
			}
			delete(videoSubscribers, subscriberCh)
			close(subscriberCh)
			if len(videoSubscribers) == 0 {
				delete(activeLiveChatSubscribers, videoId)
			}
                case <-c.publisherFinishRequestCh:
                        goto LAST
                }
	}
LAST:
	close(c.publisherFinishResponseCh)
}

func (c *Collector) start() (error) {
	if err := c.dbOperator.Open(); err != nil {
		return fmt.Errorf("can not start Collector: %w", err)
	}
	go c.publisher()
	return nil
}

func (c *Collector) stop() {
	close(c.publisherFinishRequestCh)
        <-c.publisherFinishResponseCh
}

func NewCollector(verbose bool, apiKeys []string, databasePath string) (*Collector, error) {
	if len(apiKeys) != 1 {
		return nil, fmt.Errorf("no api key")
	}
	databaseOperator, err := NewDatabaseOperator(verbose, databasePath)
	if err != nil {
		return nil, fmt.Errorf("can not create database operator: %w", err)
	}
	return &Collector {
		verbose: verbose,
		apiKey: apiKeys[0],
		dbOperator: databaseOperator,
		requestedVideoForActiveLiveChatMutex: new(sync.Mutex),
		requestedVideoForActiveLiveChat: make(map[string]bool),
		requestedVideoForArchiveLiveChatMutex: new(sync.Mutex),
		requestedVideoForArchiveLiveChat: make(map[string]bool),
		publishActiveLiveChatCh: make(chan *publishActiveLiveChatMessagesParams),
		subscribeActiveLiveChatCh: make(chan *subscribeActiveLiveChatParams),
		unsubscribeActiveLiveChatCh: make(chan *subscribeActiveLiveChatParams),
		publisherFinishRequestCh: make(chan int),
		publisherFinishResponseCh: make(chan int),
	}, nil
}
