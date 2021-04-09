package processor

import (
	"bytes"
	"fmt"
	"github.com/potix/ylcc/collector"
	"github.com/potix/ylcc/counter"
	pb "github.com/potix/ylcc/protocol"
	"github.com/psykhi/wordclouds"
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
	verbose                     bool
	collector                   *collector.Collector
	mecabrc                     string
	font                        string
	videoWordCloudMutex         *sync.Mutex
	videoWordCloud              map[string]bool
	videoWordCloudMessagesMutex *sync.Mutex
	videoWordCloudMessages      map[string][]*pb.ActiveLiveChatMessage
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

func (p *Processor) getWordCloudMessages(videoId string, target pb.Target) []string {
	p.videoWordCloudMessagesMutex.Lock()
	defer p.videoWordCloudMessagesMutex.Unlock()
	activeLiveChatMessages, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		activeLiveChatMessages = make([]*pb.ActiveLiveChatMessage, 0, 5000)
		p.videoWordCloudMessages[videoId] = activeLiveChatMessages
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
	return messages
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

func (p *Processor) registerVideoWordCloudMutex(videoId string) bool {
	p.videoWordCloudMutex.Lock()
	defer p.videoWordCloudMutex.Unlock()
	_, ok := p.videoWordCloud[videoId]
	if ok {
		return false
	}
	p.videoWordCloud[videoId] = true
	return true
}

func (p *Processor) unregisterVideoWordCloud(videoId string) {
	p.videoWordCloudMutex.Lock()
	defer p.videoWordCloudMutex.Unlock()
	_, ok := p.videoWordCloud[videoId]
	if !ok {
		return
	}
	delete(p.videoWordCloud, videoId)
}

func (p *Processor) storeWordCloudMessages(videoId string) {
	subscribeActiveLiveChatParams := p.collector.SubscribeActiveLiveChat(videoId)
	defer p.collector.UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
	for {
		response, ok := <-subscribeActiveLiveChatParams.GetSubscriberCh()
		if !ok {
			p.deleteWordCloudMessages(videoId)
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

func (p *Processor) GetWordCloud(request *pb.GetWordCloudRequest) (*pb.GetWordCloudResponse, error) {
	ok := p.registerVideoWordCloudMutex(request.VideoId)
	if ok {
		go p.storeWordCloudMessages(request.VideoId)

	}
	messages := p.getWordCloudMessages(request.VideoId, request.Target)
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
	)
	img := wordCound.Draw()
	buf := new(bytes.Buffer)
	status := new(pb.Status)
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
		verbose:                     baseOpts.verbose,
		collector:                   collector,
		mecabrc:                     mecabrc,
		font:                        font,
		videoWordCloudMutex:         new(sync.Mutex),
		videoWordCloud:              make(map[string]bool),
		videoWordCloudMessagesMutex: new(sync.Mutex),
		videoWordCloudMessages:      make(map[string][]*pb.ActiveLiveChatMessage),
	}
}
