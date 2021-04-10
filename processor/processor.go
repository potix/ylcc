package processor

import (
	"bytes"
	"fmt"
	"github.com/potix/ylcc/collector"
	"github.com/potix/ylcc/counter"
	pb "github.com/potix/ylcc/protocol"
	"github.com/psykhi/wordclouds"
	"image/color"
	"image/png"
	"sync"
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

type Processor struct {
	verbose                      bool
	collector                    *collector.Collector
	mecabrc                      string
	font                         string
	requestedVideoWordCloudMutex *sync.Mutex
	requestedVideoWordCloud      map[string]bool
	videoWordCloudMessagesMutex  *sync.Mutex
	videoWordCloudMessages       map[string][]*pb.ActiveLiveChatMessage
}

func (p *Processor) registerRequestedVideoWordCloud(videoId string) bool {
        p.requestedVideoWordCloudMutex.Lock()
        defer p.requestedVideoWordCloudMutex.Unlock()
        _, ok := p.requestedVideoWordCloud[videoId]
        if ok {
                return false
        }
        p.requestedVideoWordCloud[videoId] = true
        return true
}

func (p *Processor) checkRequestedVideoWordCloud(videoId string) bool {
        p.requestedVideoWordCloudMutex.Lock()
        defer p.requestedVideoWordCloudMutex.Unlock()
        _, ok := p.requestedVideoWordCloud[videoId]
        if ok {
                return true
        }
        return false
}

func (p *Processor) unregisterRequestedVideoWordCloud(videoId string) bool {
        p.requestedVideoWordCloudMutex.Lock()
        defer p.requestedVideoWordCloudMutex.Unlock()
        _, ok := p.requestedVideoWordCloud[videoId]
        if !ok {
                return false
        }
        delete(p.requestedVideoWordCloud, videoId)
        return true
}

func (p *Processor) addWordCloudMessage(videoId string, activeLiveChatMessage *pb.ActiveLiveChatMessage) {
	p.videoWordCloudMessagesMutex.Lock()
	defer p.videoWordCloudMessagesMutex.Unlock()
	messages, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		messages = make([]*pb.ActiveLiveChatMessage, 0, 5000)
		p.videoWordCloudMessages[videoId] = messages
	}
	messages = append(messages, activeLiveChatMessage)
}

func (p *Processor) getWordCloudMessages(videoId string, target pb.Target) ([]string, bool) {
	p.videoWordCloudMessagesMutex.Lock()
	defer p.videoWordCloudMessagesMutex.Unlock()
	activeLiveChatMessages, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		return nil, false
	}
	messages := make([]string, 0, len(activeLiveChatMessages))
	for _, activeLiveChatMessage := range activeLiveChatMessages {
		if target == pb.Target_OWNER_MODERATOR {
			if !(activeLiveChatMessage.AuthorIsChatModerator ||
				activeLiveChatMessage.AuthorIsChatOwner) {
				continue
			}
		} else if target == pb.Target_OWNER_MODERATOR_SPONSOR {
			if !(activeLiveChatMessage.AuthorIsChatModerator ||
				activeLiveChatMessage.AuthorIsChatOwner ||
				activeLiveChatMessage.AuthorIsChatSponsor) {
				continue
			}
		}
		if activeLiveChatMessage.DisplayMessage == "" {
			continue
		}

		// XX TODO 連投防止

		messages = append(messages, activeLiveChatMessage.DisplayMessage)
	}
	return messages, true
}

func (p *Processor) deleteWordCloudMessages(videoId string) {
	p.videoWordCloudMessagesMutex.Lock()
	defer p.videoWordCloudMessagesMutex.Unlock()
	_, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		return
	}
	delete(p.videoWordCloudMessages, videoId)
}

func (p *Processor) storeWordCloudMessages(videoId string) {
	subscribeActiveLiveChatParams := p.collector.SubscribeActiveLiveChat(videoId)
	defer p.collector.UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
	for {
		response, ok := <-subscribeActiveLiveChatParams.GetSubscriberCh()
		if !ok {
			p.deleteWordCloudMessages(videoId)
			p.unregisterRequestedVideoWordCloud(videoId)
			return
		}
		for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
			if activeLiveChatMessage.DisplayMessage == "" {
				continue
			}
			p.addWordCloudMessage(videoId, activeLiveChatMessage)
		}
	}
}

func (p *Processor) StartCollectionWordCloudMessages(request *pb.StartCollectionWordCloudMessagesRequest) (*pb.StartCollectionWordCloudMessagesResponse, error) {
	status := new(pb.Status)
	ok := p.registerRequestedVideoWordCloud(request.VideoId)
	if !ok {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("collecting for word cloud messages is in progress (videoId = %v)", request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: status,
			Video: nil,
		}, nil
	}
	youtubeService, err := p.collector.CreateYoutubeService()
	if err != nil {
                status.Code = pb.Code_INTERNAL_ERROR
                status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		p.unregisterRequestedVideoWordCloud(request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: status,
			Video: nil,
		}, nil
        }
	youtubeVideo, ok, err := p.collector.GetActiveVideoFromYoutube(request.VideoId, youtubeService)
        if err != nil {
                status.Code = pb.Code_INTERNAL_ERROR
                status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		p.unregisterRequestedVideoWordCloud(request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: status,
			Video: nil,
		}, nil
        }
	if !ok {
                status.Code = pb.Code_NOT_FOUND
                status.Message = fmt.Sprintf("not found videoId (videoId = %v)", request.VideoId)
		p.unregisterRequestedVideoWordCloud(request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: status,
			Video: nil,
		}, nil
        }
	video := &pb.Video{
                VideoId:            youtubeVideo.Id,
                ChannelId:          youtubeVideo.Snippet.ChannelId,
                CategoryId:         youtubeVideo.Snippet.CategoryId,
                Title:              youtubeVideo.Snippet.Title,
                Description:        youtubeVideo.Snippet.Description,
                PublishedAt:        youtubeVideo.Snippet.PublishedAt,
                Duration:           youtubeVideo.ContentDetails.Duration,
                ActiveLiveChatId:   youtubeVideo.LiveStreamingDetails.ActiveLiveChatId,
                ActualStartTime:    youtubeVideo.LiveStreamingDetails.ActualStartTime,
                ActualEndTime:      youtubeVideo.LiveStreamingDetails.ActualEndTime,
                ScheduledStartTime: youtubeVideo.LiveStreamingDetails.ScheduledStartTime,
                ScheduledEndTime:   youtubeVideo.LiveStreamingDetails.ScheduledEndTime,
                PrivacyStatus:      youtubeVideo.Status.PrivacyStatus,
                UploadStatus:       youtubeVideo.Status.UploadStatus,
                Embeddable:         youtubeVideo.Status.Embeddable,
        }
	if err := p.collector.UpdateVideo(video); err != nil {
                status.Code = pb.Code_INTERNAL_ERROR
                status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		p.unregisterRequestedVideoWordCloud(request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: status,
			Video: nil,
		}, nil
        }
	if video.ActiveLiveChatId == "" {
		status.Code = pb.Code_NOT_FOUND
                status.Message = fmt.Sprintf("not live video (videoId = %v)", request.VideoId)
		p.unregisterRequestedVideoWordCloud(request.VideoId)
                return &pb.StartCollectionWordCloudMessagesResponse{
                        Status: status,
			Video: nil,
                }, nil
	}
	go p.storeWordCloudMessages(request.VideoId)
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
        return &pb.StartCollectionWordCloudMessagesResponse{
                Status: status,
                Video:  video,
        }, nil
}

func (p *Processor) GetWordCloud(request *pb.GetWordCloudRequest) (*pb.GetWordCloudResponse, error) {
	status := new(pb.Status)
	progress := p.checkRequestedVideoWordCloud(request.VideoId)
	if !progress {
		status.Code = pb.Code_NOT_FOUND
		status.Message = fmt.Sprintf("not found word cloud messages (videoId = %v)", request.VideoId)
		return &pb.GetWordCloudResponse{
			Status:   status,
			MimeType: "",
			Data:     nil,
		}, nil
	}
	messages, ok := p.getWordCloudMessages(request.VideoId, request.Target)
	if !ok {
		status.Code = pb.Code_IN_PROGRESS
		status.Message = fmt.Sprintf("not found word cloud messages (videoId = %v)", request.VideoId)
		return &pb.GetWordCloudResponse{
			Status:   status,
			MimeType: "",
			Data:     nil,
		}, nil
	}
	verboseOpt := counter.Verbose(p.verbose)
	wordCounter := counter.NewWordCounter(p.mecabrc, verboseOpt)
	for _, message := range messages {
		wordCounter.Count(message)
	}
	result := wordCounter.Result()
	wordCound := wordclouds.NewWordcloud(
		result,
		wordclouds.FontFile(p.font),
		wordclouds.Height(int(request.Height)),
		wordclouds.Width(int(request.Width)),
		wordclouds.BackgroundColor(color.Color {
		}),
	)
	img := wordCound.Draw()
	buf := new(bytes.Buffer)
	if err := png.Encode(buf, img); err != nil {
		status.Code = pb.Code_INTERNAL_ERROR
		status.Message = fmt.Sprintf("can not create word cloud image (videoId = %v)", request.VideoId)
		return &pb.GetWordCloudResponse{
			Status:   status,
			MimeType: "",
			Data:     nil,
		}, nil
	}
	status.Code = pb.Code_SUCCESS
	status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
	return &pb.GetWordCloudResponse{
		Status:   status,
		MimeType: "image/png",
		Data:     buf.Bytes(),
	}, nil
}

func NewProcessor(collector *collector.Collector, mecabrc string, font string, opts ...Option) *Processor {
	baseOpts := defaultOptions()
	for _, opt := range opts {
		opt(baseOpts)
	}
	return &Processor{
		verbose:                      baseOpts.verbose,
		collector:                    collector,
		mecabrc:                      mecabrc,
		font:                         font,
		requestedVideoWordCloudMutex: new(sync.Mutex),
		requestedVideoWordCloud:      make(map[string]bool),
		videoWordCloudMessagesMutex:  new(sync.Mutex),
		videoWordCloudMessages:       make(map[string][]*pb.ActiveLiveChatMessage),
	}
}
