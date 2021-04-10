package processor

import (
	"bytes"
	"fmt"
	"log"
	"container/list"
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
	wordCloudMessageLimit        int
	requestedVideoWordCloudMutex *sync.Mutex
	requestedVideoWordCloud      map[string]bool
	videoWordCloudMessagesMutex  *sync.Mutex
	videoWordCloudMessages       map[string]*list.List
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
	_, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		activeLiveChatMessages := list.New()
		activeLiveChatMessages.PushBack(activeLiveChatMessage)
		p.videoWordCloudMessages[videoId] = activeLiveChatMessages
		return
	}
	if p.videoWordCloudMessages[videoId].Len() > p.wordCloudMessageLimit {
		p.videoWordCloudMessages[videoId].Remove(p.videoWordCloudMessages[videoId].Front())
	}
	p.videoWordCloudMessages[videoId].PushBack(activeLiveChatMessage)
}

func (p *Processor) getWordCloudMessages(videoId string, target pb.Target) ([]string, bool) {
	p.videoWordCloudMessagesMutex.Lock()
	defer p.videoWordCloudMessagesMutex.Unlock()
	activeLiveChatMessages, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		if p.verbose {
			log.Printf("not found word cloud message (videoId = %v)", videoId)
		}
		return nil, false
	}
	messages := make([]string, 0, activeLiveChatMessages.Len())
	for e := activeLiveChatMessages.Front(); e != nil; e = e.Next() {
		activeLiveChatMessage := e.Value.(*pb.ActiveLiveChatMessage)
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
			if p.verbose {
				log.Printf("add message for word cloud (videoId = %v,  message = %v)", videoId, activeLiveChatMessage.DisplayMessage)
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
	startCollectionActiveLiveChatRequest := &pb.StartCollectionActiveLiveChatRequest {
		VideoId: request.VideoId,
	}
	startCollectionActiveLiveChatResponse, err := p.collector.StartCollectionActiveLiveChat(startCollectionActiveLiveChatRequest)
	if err != nil {
                status.Code = pb.Code_INTERNAL_ERROR
                status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		p.unregisterRequestedVideoWordCloud(request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: status,
			Video: nil,
		}, nil
	}
	if startCollectionActiveLiveChatResponse.Status.Code != pb.Code_SUCCESS && startCollectionActiveLiveChatResponse.Status.Code != pb.Code_IN_PROGRESS {
		p.unregisterRequestedVideoWordCloud(request.VideoId)
		return &pb.StartCollectionWordCloudMessagesResponse{
			Status: startCollectionActiveLiveChatResponse.Status,
			Video: nil,
		}, nil
	}
	go p.storeWordCloudMessages(request.VideoId)
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v)", request.VideoId)
        return &pb.StartCollectionWordCloudMessagesResponse{
                Status: status,
                Video:  startCollectionActiveLiveChatResponse.Video,
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
	colors := make([]color.Color, 0, len(request.Colors))
	for _, c := range request.Colors {
		colors = append(colors, &color.RGBA{
			R: uint8(c.R),
			G: uint8(c.G),
			B: uint8(c.B),
			A: uint8(c.A),
		})
	}
	wordCound := wordclouds.NewWordcloud(
		result,
		wordclouds.FontFile(p.font),
		wordclouds.FontMaxSize(int(request.FontMaxSize)),
		wordclouds.FontMinSize(int(request.FontMinSize)),
		wordclouds.Height(int(request.Height)),
		wordclouds.Width(int(request.Width)),
		wordclouds.Colors(colors),
		wordclouds.BackgroundColor(&color.RGBA{
			R: uint8(request.BackgroundColor.R),
			G: uint8(request.BackgroundColor.G),
			B: uint8(request.BackgroundColor.B),
			A: uint8(request.BackgroundColor.A),
		}),
		wordclouds.RandomPlacement(false),
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

func NewProcessor(collector *collector.Collector, mecabrc string, font string, wordCloudMessageLimit int, opts ...Option) *Processor {
	baseOpts := defaultOptions()
	for _, opt := range opts {
		opt(baseOpts)
	}
	return &Processor{
		verbose:                      baseOpts.verbose,
		collector:                    collector,
		mecabrc:                      mecabrc,
		font:                         font,
		wordCloudMessageLimit:        wordCloudMessageLimit,
		requestedVideoWordCloudMutex: new(sync.Mutex),
		requestedVideoWordCloud:      make(map[string]bool),
		videoWordCloudMessagesMutex:  new(sync.Mutex),
		videoWordCloudMessages:       make(map[string]*list.List),
	}
}
