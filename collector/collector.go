package collector

import (
        "fmt"
        "log"
        "sync"
        "io"
        "google.golang.org/api/youtube/v3"
	"github.com/potix/ylcc/youtubehelper"
	pb "github.com/potix/ylcc/protocol"

)

const (
        bulkMessageMax int64 = 2000
)

type Collector struct {
	verbose                               bool
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
	activeLiveChatCollector               *youtubehelper.ActiveLiveChatCollector
	archiveLiveChatCollector              *youtubehelper.ArchiveLiveChatCollector

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

func (s *subscribeActiveLiveChatParams) GetSubscriberCh() (chan *pb.PollActiveLiveChatResponse) {
	return s.subscriberCh
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

func (c *Collector) collectActiveLiveChatFromYoutube(video *youtube.Video, youtubeService *youtube.Service) {
	params, err := c.activeLiveChatCollector.CreateParams(video)
	if err != nil {
		c.publishActiveLiveChatCh <- &publishActiveLiveChatMessagesParams {
			err: err,
			videoId: video.Id,
			activeLiveChatMessages: nil,
		}
		c.unregisterRequestedVideoForActiveLiveChat(video.Id)
		log.Printf("can not create params of active live chat collector: %v\n", err)
                return
	}
        for {
                liveChatMessageListResponse, err :=  c.activeLiveChatCollector.GetActiveLiveChat(params, youtubeService, bulkMessageMax)
                if err != nil {
			c.publishActiveLiveChatCh <-&publishActiveLiveChatMessagesParams {
				err: err,
				videoId: video.Id,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(video.Id)
			if c.verbose {
				log.Printf("can not get live chat messages: %v\n", err)
			}
                        return
                }
		activeLiveChatMessages := make([]*pb.ActiveLiveChatMessage, 0, bulkMessageMax)
                for _, item := range liveChatMessageListResponse.Items {
                        if item.Snippet.SuperChatDetails != nil {
				activeLiveChatMessage := &pb.ActiveLiveChatMessage{
					MessageId: item.Id,
					ChannelId: video.Snippet.ChannelId,
					VideoId: video.Id,
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
					PageToken: params.GetPageToken(),
				}
				activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
                        } else if item.Snippet.TextMessageDetails != nil {
				activeLiveChatMessage := &pb.ActiveLiveChatMessage{
					MessageId: item.Id,
					ChannelId: video.Snippet.ChannelId,
					VideoId: video.Id,
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
					PageToken: params.GetPageToken(),
				}
				activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
                        }
                }
		if err := c.dbOperator.UpdateActiveLiveChatMessages(activeLiveChatMessages); err != nil {
			c.publishActiveLiveChatCh <- &publishActiveLiveChatMessagesParams {
				err: err,
				videoId: video.Id,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(video.Id)
			log.Printf("can not update active live chat messages in database: %v\n", err)
                        return
                }
		c.publishActiveLiveChatCh <- &publishActiveLiveChatMessagesParams {
			err: nil,
			videoId: video.Id,
			activeLiveChatMessages: activeLiveChatMessages,
		}
		ok := c.activeLiveChatCollector.Next(params, liveChatMessageListResponse)
		if !ok {
			c.publishActiveLiveChatCh <- &publishActiveLiveChatMessagesParams {
				err: io.EOF,
				videoId: video.Id,
				activeLiveChatMessages: nil,
			}
			c.unregisterRequestedVideoForActiveLiveChat(video.Id)
		}
        }
}

func (c *Collector) GetVideo(request *pb.GetVideoRequest) (*pb.GetVideoResponse, error) {
	status := new(pb.Status)
	video, ok, err := c.dbOperator.GetVideoByVideoId(request.VideoId)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		return &pb.GetVideoResponse {
			Status: status,
			Video: video,
		}, nil
	}
	if !ok {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		return &pb.GetVideoResponse {
			Status: status,
			Video: video,
		}, nil
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.GetVideoResponse {
		Status: status,
		Video: video,
	}, nil
}

func (c *Collector) StartCollectionActiveLiveChat(request *pb.StartCollectionActiveLiveChatRequest) (*pb.StartCollectionActiveLiveChatResponse, error) {
	status := new(pb.Status)
	ok := c.registerRequestedVideoForActiveLiveChat(request.VideoId)
	if !ok {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	youtubeService, err := c.activeLiveChatCollector.CreateYoutubeService()
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	youtubeVideo, ok, err := c.activeLiveChatCollector.GetVideo(request.VideoId, youtubeService)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	if !ok {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	video := &pb.Video {
                VideoId: youtubeVideo.Id,
                ChannelId: youtubeVideo.Snippet.ChannelId,
                CategoryId: youtubeVideo.Snippet.CategoryId,
                Title: youtubeVideo.Snippet.Title,
                Description: youtubeVideo.Snippet.Description,
                PublishedAt: youtubeVideo.Snippet.PublishedAt,
                Duration: youtubeVideo.ContentDetails.Duration,
                ActiveLiveChatId: youtubeVideo.LiveStreamingDetails.ActiveLiveChatId,
                ActualStartTime: youtubeVideo.LiveStreamingDetails.ActualStartTime,
                ActualEndTime: youtubeVideo.LiveStreamingDetails.ActualEndTime,
                ScheduledStartTime: youtubeVideo.LiveStreamingDetails.ScheduledStartTime,
                ScheduledEndTime: youtubeVideo.LiveStreamingDetails.ScheduledEndTime,
                PrivacyStatus: youtubeVideo.Status.PrivacyStatus,
                UploadStatus: youtubeVideo.Status.UploadStatus,
                Embeddable : youtubeVideo.Status.Embeddable,
        }
        err = c.dbOperator.UpdateVideo(video)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}, nil
	}
	if video.ActiveLiveChatId == "" {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not live video (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.StartCollectionActiveLiveChatResponse {
			Status: status,
			Video: video,
		}, nil
        }
	go c.collectActiveLiveChatFromYoutube(youtubeVideo, youtubeService)
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.StartCollectionActiveLiveChatResponse {
		Status: status,
		Video: video,
	}, nil
}

func (c *Collector) GetCachedActiveLiveChat(request *pb.GetCachedActiveLiveChatRequest) (*pb.GetCachedActiveLiveChatResponse, error) {
	status := new(pb.Status)
	progress := c.checkRequestedVideoForActiveLiveChat(request.VideoId)
	if progress {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v, offset = %v, count = %v)", request.VideoId, request.Offset, request.Count)
		return &pb.GetCachedActiveLiveChatResponse {
			Status: status,
			ActiveLiveChatMessages: nil,
		}, nil
	}
	activeLiveChatMessages, err :=  c.dbOperator.GetActiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.Offset, request.Count)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v, offset = %v, count = %v)", err, request.VideoId, request.Offset, request.Count)
		c.unregisterRequestedVideoForActiveLiveChat(request.VideoId)
		return &pb.GetCachedActiveLiveChatResponse {
			Status: status,
			ActiveLiveChatMessages: nil,
		}, nil
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v, offset = %v, count = %v)", request.VideoId, request.Offset, request.Count)
	return &pb.GetCachedActiveLiveChatResponse {
		Status: status,
		ActiveLiveChatMessages: activeLiveChatMessages,
	}, nil
}

func (c *Collector) collectArchiveLiveChatFromYoutube(channelId string, videoId string) {
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
		archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0, bulkMessageMax)
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
		ok := c.archiveLiveChatCollector.Next(params, resp)
                if !ok {
                        break
                }
        }
        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
}

func  (c *Collector) StartCollectionArchiveLiveChat(request *pb.StartCollectionArchiveLiveChatRequest) (*pb.StartCollectionArchiveLiveChatResponse, error) {
	status := new(pb.Status)
	ok := c.registerRequestedVideoForArchiveLiveChat(request.VideoId)
	if !ok {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	youtubeService, err := c.activeLiveChatCollector.CreateYoutubeService()
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	youtubeVideo, ok, err := c.activeLiveChatCollector.GetVideo(request.VideoId, youtubeService)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	if !ok {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: nil,
		}, nil
	}
	video := &pb.Video {
                VideoId: youtubeVideo.Id,
                ChannelId: youtubeVideo.Snippet.ChannelId,
                CategoryId: youtubeVideo.Snippet.CategoryId,
                Title: youtubeVideo.Snippet.Title,
                Description: youtubeVideo.Snippet.Description,
                PublishedAt: youtubeVideo.Snippet.PublishedAt,
                Duration: youtubeVideo.ContentDetails.Duration,
                ActiveLiveChatId: youtubeVideo.LiveStreamingDetails.ActiveLiveChatId,
                ActualStartTime: youtubeVideo.LiveStreamingDetails.ActualStartTime,
                ActualEndTime: youtubeVideo.LiveStreamingDetails.ActualEndTime,
                ScheduledStartTime: youtubeVideo.LiveStreamingDetails.ScheduledStartTime,
                ScheduledEndTime: youtubeVideo.LiveStreamingDetails.ScheduledEndTime,
                PrivacyStatus: youtubeVideo.Status.PrivacyStatus,
                UploadStatus: youtubeVideo.Status.UploadStatus,
                Embeddable : youtubeVideo.Status.Embeddable,
        }
        err = c.dbOperator.UpdateVideo(video)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}, nil
	}
	if video.ActiveLiveChatId != "" {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not archive video(videoId = %v)", request.VideoId)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.StartCollectionArchiveLiveChatResponse {
			Status: status,
			Video: video,
		}, nil
        }
	if !request.Replace {
                count, err := c.dbOperator.CountArchiveLiveChatMessagesByVideoId(request.VideoId)
                if err != nil {
			status.Code = pb.Code_INTERNAL_ERROR
			status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
			c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
			return &pb.StartCollectionArchiveLiveChatResponse {
				Status: status,
				Video: video,
			}, nil
                }
                if count > 0 {
                        if c.verbose {
                                log.Printf("already exists archive live chat in database (videoId = %v)", request.VideoId)
                        }
                        c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
			status.Code = pb.Code_SUCCESS
			status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
			return &pb.StartCollectionArchiveLiveChatResponse {
				Status: status,
				Video: video,
			}, nil
                }
        }
	go c.collectArchiveLiveChatFromYoutube(video.ChannelId, video.VideoId)
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.StartCollectionArchiveLiveChatResponse {
		Status: status,
		Video: video,
	}, nil
}

func (c *Collector) GetArchiveLiveChat(request *pb.GetArchiveLiveChatRequest) (*pb.GetArchiveLiveChatResponse, error) {
	status := new(pb.Status)
	progress := c.checkRequestedVideoForArchiveLiveChat(request.VideoId)
	if progress {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("be in progress (videoId = %v, offset = %v, count = %v)", request.VideoId, request.Offset, request.Count)
		return &pb.GetArchiveLiveChatResponse {
			Status: status,
			ArchiveLiveChatMessages: nil,
		}, nil
	}
	archiveLiveChatMessages, err :=  c.dbOperator.GetArchiveLiveChatMessagesByVideoIdAndToken(request.VideoId, request.Offset, request.Count)
	if err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("%v (videoId = %v, offset = %v, count = %v)", err, request.VideoId, request.Offset, request.Count)
		c.unregisterRequestedVideoForArchiveLiveChat(request.VideoId)
		return &pb.GetArchiveLiveChatResponse {
			Status: status,
			ArchiveLiveChatMessages: nil,
		}, nil
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v, offset = %v, count)", request.VideoId, request.Offset, request.Count)
	return &pb.GetArchiveLiveChatResponse {
		Status: status,
		ArchiveLiveChatMessages: archiveLiveChatMessages,
	}, nil
}

func (c *Collector) SubscribeActiveLiveChat(videoId string) (*subscribeActiveLiveChatParams) {
        subscribeActiveLiveChatParams := &subscribeActiveLiveChatParams {
                videoId: videoId,
                subscriberCh: make(chan *pb.PollActiveLiveChatResponse),
        }
	c.subscribeActiveLiveChatCh <-subscribeActiveLiveChatParams
	return subscribeActiveLiveChatParams
}

func (c *Collector) UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams *subscribeActiveLiveChatParams) {
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

func (c *Collector) Start() (error) {
	if err := c.dbOperator.Open(); err != nil {
		return fmt.Errorf("can not start Collector: %w", err)
	}
	go c.publisher()
	return nil
}

func (c *Collector) Stop() {
	close(c.publisherFinishRequestCh)
        <-c.publisherFinishResponseCh
}

func NewCollector(apiKeys []string, databasePath string, opts ...Option) (*Collector, error) {
	baseOpts := defaultOptions()
        for _, opt := range opts {
                opt(baseOpts)
        }
	if len(apiKeys) > 0 {
		return nil, fmt.Errorf("no api key")
	}
	verboseOpt := Verbose(baseOpts.verbose)
	databaseOperator, err := NewDatabaseOperator(databasePath, verboseOpt)
	if err != nil {
		return nil, fmt.Errorf("can not create database operator: %w", err)
	}
	ythVerboseOpt := youtubehelper.Verbose(baseOpts.verbose)
	return &Collector {
		verbose: baseOpts.verbose,
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
		activeLiveChatCollector: youtubehelper.NewActiveLiveChatCollector(apiKeys[0], ythVerboseOpt),
		archiveLiveChatCollector: youtubehelper.NewArchiveLiveChatCollector(ythVerboseOpt),

	}, nil
}
