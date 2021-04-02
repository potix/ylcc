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
	archiveLiveChatCollector              *ArchiveLiveChatCollector
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
        videosListCall := youtubeService.Videos.List([]string{"snippet", "contentDetails", "liveStreamingDetails", "status"})
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
		activeLiveChatMessages := make([]*pb.ActiveLiveChatMessage, 0, messageMax)
                for _, item := range liveChatMessageListResponse.Items {
                        if item.Snippet.SuperChatDetails != nil {
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
					DisplayMessage: item.Snippet.SuperChatDetails.UserComment,
					PublishedAt: item.Snippet.PublishedAt,
					IsSuperChat: true,
					AmountDisplayString: item.Snippet.SuperChatDetails.AmountDisplayString,
					Currency: item.Snippet.SuperChatDetails.AmountDisplayString,
					PageToken: pageToken,
				}
				activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
                        } else if item.Snippet.TextMessageDetails != nil {
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
					DisplayMessage: item.Snippet.TextMessageDetails.MessageText,
					PublishedAt: item.Snippet.PublishedAt,
					IsSuperChat: false,
					AmountDisplayString: "",
					Currency: "",
					PageToken: pageToken,
				}
				activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
                        }
                }
		if err := c.dbOperator.UpdateActiveLiveChatMessages(activeLiveChatMessages); err != nil {
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
		pageToken = liveChatMessageListResponse.NextPageToken
                time.Sleep(time.Duration(liveChatMessageListResponse.PollingIntervalMillis) * time.Millisecond)
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
		status.Message = fmt.Sprintf("be in progress (videoId = %v, offset = %v, count = %v)", request.VideoId, request.Offset, request.Count)
		return &pb.GetCachedActiveLiveChatResponse {
			Status: status,
			ActiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	activeLiveChatMessages, err :=  c.dbOperator.GetActiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.Offset, request.Count)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v, offset = %v, count = %v)", err, request.VideoId, request.Offset, request.Count)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.GetCachedActiveLiveChatResponse {
			Status: status,
			ActiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v, offset = %v, count = %v)", request.VideoId, request.Offset, request.Count)
	return &pb.GetCachedActiveLiveChatResponse {
		Status: status,
		ActiveLiveChatMessages: activeLiveChatMessages,
	}, nil
}

func (c *Collector) collectArchiveLiveChatFromYoutube(channelId string, videoId string, replace bool) {
	if !replace {
                count, err := c.dbOperator.CountArchiveLiveChatMessagesByVideoId(videoId)
                if err != nil {
                        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                        log.Printf("can not get archive live chat from database (videoId = %v): %v", videoId, err)
                        return
                }
                if count > 0 {
                        if c.verbose {
                                log.Printf("already exists archive live chat in database (videoId = %v)", videoId)
                        }
                        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                        return
                }
        }
	params, err := c.archiveLiveChatCollector.GetParams(videoId)
        if err != nil {
                log.Printf("can not get params of archive live chat: %v", err)
                c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                return
        }
	for {
                resp, err := c.archiveLiveChatCollector.GetArchiveLiveChat(params)
                if err != nil {
                        log.Printf("can not get archive live chat: %v", err)
			c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                        return
                }
		archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0, messageMax)
		for _, cact := range resp.ContinuationContents.LiveChatContinuation.Actions {
                        for _, iact := range cact.ReplayChatItemAction.Actions {
                                if iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.ID != "" {
					messageText := ""
                                        for _, run := range iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.Message.Runs {
                                                messageText += run.Text
					}
					archiveLiveChatMessage := &pb.ArchiveLiveChatMessage{
						MessageId: iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.ID,
						ChannelId: channelId,
						VideoId: videoId,
						ClientId: iact.AddChatItemAction.ClientID,
						AuthorName: iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.AuthorName.SimpleText,
						AuthorExternalChannelId: iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.AuthorExternalChannelID,
						MessageText: messageText,
						PurchaseAmountText: iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.PurchaseAmountText.SimpleText,
						IsPaid: true,
						TimestampUsec: iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampUsec,
						TimestampText: iact.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampText.SimpleText,
						VideoOffsetTimeMsec: cact.ReplayChatItemAction.VideoOffsetTimeMsec,
						Continuation: params.GetContinuation(),
					}
					archiveLiveChatMessages = append(archiveLiveChatMessages, archiveLiveChatMessage)
                                }
				if iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.ID != "" {
					messageText := ""
                                        for _, run := range iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.Message.Runs {
                                                messageText += run.Text
                                        }
					archiveLiveChatMessage := &pb.ArchiveLiveChatMessage{
						MessageId: iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.ID,
						ChannelId: channelId,
						VideoId: videoId,
						ClientId: iact.AddChatItemAction.ClientID,
						AuthorName: iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.AuthorName.SimpleText,
						AuthorExternalChannelId: iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.AuthorExternalChannelID,
						MessageText: messageText,
						PurchaseAmountText: "",
						IsPaid: false,
						TimestampUsec: iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampUsec,
						TimestampText: iact.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampText.SimpleText,
						VideoOffsetTimeMsec: cact.ReplayChatItemAction.VideoOffsetTimeMsec,
						Continuation: params.GetContinuation(),
					}
					archiveLiveChatMessages = append(archiveLiveChatMessages, archiveLiveChatMessage)
                                }
                        }
                }
		if err := c.dbOperator.UpdateArchiveLiveChatMessages(archiveLiveChatMessages); err != nil {
			c.unregisterRequestedVideoForArchiveLiveChat(videoId)
			log.Printf("can not update archive live chat messages in database: %v\n", err)
                        return
                }
		next := c.archiveLiveChatCollector.Next(params, resp)
                if !next {
                        break
                }
        }
        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
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
		status.Message = fmt.Sprintf("be in progress (videoId = %v, offset = %v, count = %v)", request.VideoId, request.Offset, request.Count)
		return &pb.GetArchiveLiveChatResponse {
			Status: status,
			ArchiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	archiveLiveChatMessages, err :=  c.dbOperator.GetArchiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.Offset, request.Count)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v, offset = %v, count = %v)", err, request.VideoId, request.Offset, request.Count)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.GetArchiveLiveChatResponse {
			Status: status,
			ArchiveLiveChatMessages: nil,
		}, fmt.Errorf("%v", status.Message)
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v, offset = %v, count)", request.VideoId, request.Offset, request.Count)
	return &pb.GetArchiveLiveChatResponse {
		Status: status,
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
		archiveLiveChatCollector: NewArchiveLiveChatCollector(),
	}, nil
}
