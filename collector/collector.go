package collector

import (
        "os"
        "strings"
        "io/ioutil"
)

const (
	messageMax int = 2000
	retryMax int = 10
)


type Collector struct {
	verbose                     bool
	apiKey                      string
	dbOperator                  *DatabaseOperator
	requestedVideoForActiveLiveChatMutex         *sync.Mutex
	requestedVideoForActiveLiveChat	            map[string]bool
	publishActiveLiveChatCh     *publishActiveLiveChatMessagesParams
	subscribeActiveLiveChatCh   *subscribeActiveLiveChatParams
	unsubscribeActiveLiveChatCh *subscribeActiveLiveChatParams
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

func (c *collector) registerRequestedVideoForActiveLiveChat(videoId string) (bool) {
	c.requestedVideoForActiveLiveChatMutex.Lock()
	defer requestedVideoForActiveLiveChatMutex.Unlock()
	_, ok := c.requestedForActiveLiveChatVideo[videoId]
	if ok {
		return false
	}
	c.requestedVideoForActiveLiveChat[videoId] = true
	return true
}

func (c *collector) checkRequestedVideoForActiveLiveChat(videoId string) (bool) {
	c.requestedVideoForActiveLiveChatMutex.Lock()
	defer requestedVideoForActiveLiveChatMutex.Unlock()
	_, ok := c.requestedForActiveLiveChatVideo[videoId]
	if ok {
		return true
	}
	return false
}

func (c *Collector) unregisterRequestedVideoForActiveLiveChat(videoId string) (bool) {
	c.requestedVideoForActiveLiveChatMutex.Lock()
	defer requestedVideoForActiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForActiveLiveChat[videoId]
	if !ok {
		return false
	}
	delete(c.requestedVideoForActiveLiveChat, videoId)
	return true
}


func (c *Collector) registerRequestedVideoForArchiveLiveChat(videoId string) (bool) {
	c.requestedVideoForArchiveLiveChatMutex.Lock()
	defer requestedVideoForArchiveLiveChatMutex.Unlock()
	_, ok := c.requestedForArchiveLiveChatVideo[videoId]
	if ok {
		return false
	}
	c.requestedVideoForArchiveLiveChat[videoId] = true
	return true
}

func (c *Collector) checkRequestedVideoForArchiveLiveChat(videoId string) (bool) {
	c.requestedVideoForArchiveLiveChatMutex.Lock()
	defer requestedVideoForArchiveLiveChatMutex.Unlock()
	_, ok := c.requestedForArchiveLiveChatVideo[videoId]
	if ok {
		return true
	}
	return false
}

func (c *Collector) unregisterRequestedVideoForArchiveLiveChat(videoId string) (bool) {
	c.requestedVideoForArchiveLiveChatMutex.Lock()
	defer requestedVideoForArchiveLiveChatMutex.Unlock()
	_, ok := c.requestedVideoForArchiveLiveChat[videoId]
	if !ok {
		return false
	}
	delete(c.requestedVideoForArchiveLiveChat, videoId)
	return true
}

func (c *Collector) createYoutubeService() (youtubeService *youtube.Service, error) {
        ctx := context.Background()
        newClient, _, err := http.NewClient(ctx, option.WithAPIKey(c.apiKey))
        if err != nil {
		return nil, nil, fmt.Errorf("can not create http clinet: %w", err)
        }
        youtubeService, err := youtube.New(newClient)
        if err != nil {
		return nil, nil, fmt.Errorf("can not create youtube service: %w", err)
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
        video := &pb.Video {
                VideoId: item.Id,
                ChannelId: item.Snippet.ChannelId,
                CategoryId: item.SnippetCategoryId,
                Title: item.Snippet.Title,
                Description: item.Snippet.Description,
                PublishdAt: item.Snippet.PublishedAt,
                Duration: item.ContentDetails.Duration,
                ActiveLiveChatId: item.LiveStreamingDetails.ActiveLiveChatId,
                ActualStartTime: item.LiveStreamingDetails.ActualStartTime,
                ActualEndTime: item.LiveStreamingDetails.ActualEndTime,
                ScheduledStartTime: item.LiveStreamingDetails.ScheduledStartTime,
                ScheduledEndTime: item.LiveStreamingDetails.ScheduledEndTime,
                StatusPrivacyStatus: item.Status.PrivacyStatus,
                StatusUploadStatus: item.Status.UploadStatus,
                StatusEmbeddable : item.Status.Embeddable,
                ResponseEtag: videoListResponse.Etag,
        }
        return video, true, nil
}

func (c *Collector) collectActiveLiveChatFromYoutube(channelId string, videoId string, youtubeService *youtube.Service) {
        pageToken := ""
        for {
                liveChatMessagesListCall := youtubeService.LiveChatMessages.List(video.ActiveLiveChatId, []string{"snippet", "authorDetails"})
                liveChatMessagesListCall.PageToken(pageToken)
                liveChatMessagesListCall.MaxResults(messageMax)
                liveChatMessageListResponse, err := liveChatMessagesListCall.Do()
                if err != nil {
			c.publishActiveLiveChatCh <- &publishActiveLiveChatMessages {
				err: err,
				videoId: videoId,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(videoId)
			c.verbose {
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
			isSuperChat = false
                        if item.Snippet.SuperChatDetails != nil {
				message = item.Snippet.SuperChatDetails.UserComment
                                amountDisplayString = item.Snippet.SuperChatDetails.AmountDisplayString
                                currency = item.Snippet.SuperChatDetails.AmountDisplayString
				isSuperChat = true
                        } else if item.Snippet.TextMessageDetails != nil {
                                messages = tem.Snippet.TextMessageDetails.MessageText)
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
				LiveChatId: item.Snippet.liveChatId,
				DisplayMessage: message,
				PublishedAt: item.Snippet.PublishedAt,
				IsSuperChat: isSuperChat,
				AmountDisplayString: amountDisplayString,
				Currency: currency,
			}
			activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
                }
		err = c.dbOperator.UpdateActiveLiveChatMessages(pageToken, nextPageToken, activeLiveChatMessages)
                if err != nil {
			c.publishActiveLiveChatCh <- &publishActiveLiveChatMessages {
				err: err,
				videoId: videoId,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(videoId)
			log.Printf("can not update active live chat messages in database: %v\n", err)
                        return
                }
		c.publishActiveLiveChatCh <- &publishActiveLiveChatMessages {
			err: nil,
			videoId: videoId,
			activeLiveChatMessages: activeLiveChatMessages,
		}
                time.Sleep(time.Duration(pollingIntervalMillis) * time.Millisecond)
		pageToken = nextPageToken
        }
}

func (c *Collector) GetVideo(request *pb.GetVideoRequest) (*pb.GetVideoResponse, error) {
	status := new(*pb.Status)
	video, ok, err := c.dbOperator.GetVideoByVideoId(request.VideoId)
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		return &pb.GetVideoResponse {
			Status: status,
			Video: video,
		}
	}
	if !ok {
		status.Code = Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		return &pb.GetVideoResponse {
			Status: status,
			Video: video,
		}
	}
	status.Code = Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.GetVideoResponse {
		Status: status,
		Video: video,
	}
}

func (c *Collector) StartCollectionActiveLiveChat(request *StartCollectionActiveLiveChatRequest) (*StartCollectionActiveLiveChatResponse, error) {
	status := new(*pb.Status)
	ok := c.registerRequestedVideoForActiveLiveChat(request.VideoId)
	if !ok {
		status.Code = Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	youtubeService, err := createYoutubeService()
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	video, ok, err := c.getVideoFromYoutube(request.VideoId, youtubeService)
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	if !ok {
		status.Code = Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
        err = c.dbOperator.UpdateVideo(video)
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	if video.ActiveLiveChatId == "" {
		status.Code = Code_NOT_FOUND
		status.Message = fmt.Sprintf("not live video (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}
        }
	go c.collectActiveLiveChatFromYoutube(video.ChannelId, video.VideoId, youtubeService)
	status.Code = Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.tartCollectionActiveLiveChatResponse {
		Status: status,
		Video: video,
	}
}

func (c *Collector) GetCachedActiveLiveChat(request *GetCachedActiveLiveChatRequest) (*GetCachedActiveLiveChatResponse, error) {
	status := new(*pb.Status)
	progress := c.checkRequestedVideoForActiveLiveChat(request.VideoId)
	if progress {
		status.Code = Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v, pageToken = %v)", request.VideoId, request.pageToken)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			NextPageToken: "",
			ActiveLiveChatMessage: nil,
		}
	}
	nextPageToken, activeLiveChatMessage, err :=  c.dbOperator.GetActiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.pageToken)
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v, pageToken = %v)", err, request.VideoId, request.pageToken)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			NextPageToken: "",
			ActiveLiveChatMessage: nil,
		}
	}
	status.Code = Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v, pageToken = %v)", request.VideoId, request.pageToken)
	return &pb.GetCachedActiveLiveChatResponse {
		Status: status,
		NextPageToken: nextPageToken,
		ActiveLiveChatMessage: activeLiveChatMessage,
	}
}

func  (c *Controller) StartCollectionArchiveLiveChat(request *StartCollectionArchiveLiveChatRequest) (*StartCollectionArchiveLiveChatResponse, error) {
	status := new(*pb.Status)
	ok := c.registerRequestedVideoForArchiveLiveChat(request.VideoId)
	if !ok {
		status.Code = Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	youtubeService, err := createYoutubeService()
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	video, ok, err := c.getVideoFromYoutube(request.VideoId, youtubeService)
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	if !ok {
		status.Code = Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
        err = c.dbOperator.UpdateVideo(video)
	if err != nil {
		status.Code = Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}
	}
	if video.ActiveLiveChatId != "" {
		status.Code = Code_NOT_FOUND
		status.Message = fmt.Sprintf("not archive video(videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}
        }
	go c.collectArchiveLiveChatFromYoutube(video.ChannelId, video.VideoId, request.Replace)
	status.Code = Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.StartCollectionArchiveLiveChatResponse {
		Status: status,
		Video: video,
	}
}





func (c *Collector) GetArchiveLiveChat(request *GetArchiveLiveChatRequest) (*GetArchiveLiveChatResponse, error) {
        return h.collector.GetArchiveLiveChat(request)
}









func (c *Collector) SubscribeActiveLiveChat(request *pb.PollActiveLiveChatRequest) {
        subscribeActiveLiveChatParams := &subscribeActiveLiveChatParams {
                videoId: request.VideoId,
                subscriberCh: make(chan *pb.PollActiveLiveChatResponse),
        }
	c.subscribeActiveLiveChatCh <- subscribeActiveLiveChatParams
	return subscribeActiveLiveChatParams
}

func (c *Collector) UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams *subscribeActiveLiveChatParams) {
	c.unsubscribeActiveLiveChatCh <-subscribeActiveLiveChatParams;
}

func (c *Collector) Publisher() {
	activeLiveChatSubscribers := make(map[videoId]map[chan *pb.PollActiveLiveChatResponse]bool)
	for {
                select {
		case publishActiveLiveChatMessagesParams := <-c.publishActiveLiveChatCh:
			err := publishActiveLiveChatMessagesParams.err
			videoId := publishActiveLiveChatMessagesParams.videoId
			activeLiveChatMessages := publishActiveLiveChatMessagesParams.activeLiveChatMessages
			videoSubscribers, ok := activeLiveChatSubscribers[videoId]
			if !ok {
				if c.verbose {
					log.Printf("no subscribers for active live chat. no videoId. (videoId = %v, subscriberCh = %v)", videoId, subscriberCh)
				}
				break
			}
			if err != nil {
				for subscriberCh, _ := range activeLiveChatSubscribers[videoId] {
					delete(activeLiveChatSubscribers[videoId], subscriberCh)
					close(subscriberCh)
				}
				delete(activeLiveChatSubscribers, videoId)
				break
			}
			for subscriberCh, _ := range activeLiveChatSubscribers[videoId] {
				subscriberCh <- &pb.PollActiveLiveChatResponse {
					Status: Code_SUCCESS,
					ActiveLiveChatMessages: publishActiveLiveChatMessages,
				}
			}
		case subscribeActiveLiveChatParams := <-subscribeActiveLiveChatCh:
			videoId := subscribeActiveLiveChatParams.videoId
			subscriberCh := subscribeActiveLiveChatParams.subscriberCh
			videoSubscribers, ok := activeLiveChatSubscribers[videoId]
			if !ok {
				newVideoSubscribers := make(chan *pb.PollActiveLiveChatResponse)
				activeLiveChatSubscribers[videoId] = newVideoSubscribers
			}
			activeLiveChatSubscribers[videoId][subscriberCh] = true
		case subscribeActiveLiveChatParams := <-unsubscribeActiveLiveChatCh:
			videoId := activeLiveChatSubscribe.videoId
			subscriberCh := activeLiveChatSubscribe.subscriberCh
			videoSubscribers, ok := activeLiveChatSubscribers[videoId]
			if !ok {
				if c.verbose {
					log.Printf("no subscribers for active live chat. no videoId. (videoId = %v, subscriberCh = %v)", videoId, subscriberCh)
				}
				break
			}
			_, ok activeLiveChatSubscribers[videoId][subscriberCh]
			if !ok {
				if c.verbose {
					log.Printf("no subscriber for active live chat. no subscriber channel. (videoId = %v, subscriberCh = %v)", videoId, subscriberCh)
				}
				break
			}
			delete(activeLiveChatSubscribers[videoId], subscriberCh)
			close(subscriberCh)
			if len(activeLiveChatSubscribers[videoId]) == 0 {
				delete(activeLiveChatSubscribers, videoId)
			}
                case <-c.publisherFinishRequestCh:
                        goto LAST
                }
	}
LAST:
	close(c.publisherFinishResponseCh)
}

func (c *Collector) Start() (error) {
	err := c.dbOperator.Start()
	if err != nil {
		return fmt.Errorf("can not start collector: %w", err)
	}
	go c.publisher()
}

func (c *Collector) Stop() {
	close(c.publisherFinishResquestChan)
        <-c.publisherFinishResponseChan
}

func NewCollector(verbose bool, apiKeys []string, databasePath string) (*Collector, error) {
	if len(apiKeys) != 1 {
		return nil, fmt.Errorf("no api key")
	}
	databaseOperator, err := NewDatabaseOperator(verbose, databasePath)
	if err != nil {
		return nil, fmt.Errorf("can not create database operator: %w", err)
	}
	return &collector {
		verbose: verbose,
		apiKey: apiKeys[0],
		dbOperator: databaseOperator,
		requestedVideoMutex: new(sync.Mutex),
		requestedVideo: make(map[string]string),
		publishActiveLiveChatCh: make(chan *publishActiveLiveChatMessages)
	}
}
