package processor

import (
	"bytes"
	"fmt"
	"log"
	"time"
	"strings"
	"regexp"
	"sort"
	"github.com/potix/ylcc/collector"
	"github.com/potix/ylcc/counter"
	pb "github.com/potix/ylcc/protocol"
	"github.com/psykhi/wordclouds"
	"image/color"
	"image/png"
	"sync"
	"github.com/google/uuid"
	"crypto/sha1"
	"golang.org/x/text/unicode/norm"
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
	requestedVoteMutex           *sync.Mutex
	requestedVote                map[string]*voteContext
	requestedGroupingMutex       *sync.Mutex
	requestedGrouping            map[string]*groupingContext
	stampRe                      *regexp.Regexp
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
	activeLiveChatMessages, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		activeLiveChatMessages := make([]*pb.ActiveLiveChatMessage, 0, 2000)
		activeLiveChatMessages = append(activeLiveChatMessages, activeLiveChatMessage)
		p.videoWordCloudMessages[videoId] = activeLiveChatMessages
		return
	}
	p.videoWordCloudMessages[videoId] = append(activeLiveChatMessages, activeLiveChatMessage)
}

func (p *Processor) getWordCloudMessages(videoId string, target pb.Target, messageLimit int) ([]string, bool) {
	p.videoWordCloudMessagesMutex.Lock()
	defer p.videoWordCloudMessagesMutex.Unlock()
	activeLiveChatMessages, ok := p.videoWordCloudMessages[videoId]
	if !ok {
		if p.verbose {
			log.Printf("not found word cloud message (videoId = %v)", videoId)
		}
		return nil, false
	}
	messages := make([]string, 0, len(activeLiveChatMessages))
	for i := len(activeLiveChatMessages) - 1; i >= 0 && i >= len(activeLiveChatMessages) - 1 - messageLimit; i -= 1  {
		activeLiveChatMessage := activeLiveChatMessages[i];
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
		// XXX TODO 連投防止
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
	subscribeActiveLiveChatParams, err := p.collector.SubscribeActiveLiveChat(videoId)
	if err != nil {
		if p.verbose {
			log.Printf("can not subsctibe (videoId = %v)", videoId)
		}
		p.deleteWordCloudMessages(videoId)
		p.unregisterRequestedVideoWordCloud(videoId)
		return
	}
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
	// XXX if startCollectionActiveLiveChatResponse.Status.Code == pb.Code_IN_PROGRESS: getVideo
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
	messages, ok := p.getWordCloudMessages(request.VideoId, request.Target, int(request.MessageLimit))
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
	if p.verbose {
		log.Printf("%+v", result)
	}
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

type voteContext struct {
	voteId              string
	videoId             string
	target              pb.Target
	duration            int32
	choices             []*pb.VoteChoice
	stopTimer           *time.Timer
	watcherResetEventCh chan int32
	watcherCloseEventCh chan int
	total               int32
	counts              []*pb.VoteCount
	voted               map[string]bool
	stopped             bool
}

func (v *voteContext) setStopTimer() {
	v.stopTimer = time.NewTimer(time.Duration(v.duration) * time.Second)
}

func (v *voteContext) resetStopTimer(duration int32) {
	v.stopTimer.Stop()
	v.duration = duration
	v.stopTimer = time.NewTimer(time.Duration(duration) * time.Second)
}

func (v *voteContext) emitWatcherResetEvent(duration int32) {
	if v.stopped {
		return
	}
	v.watcherResetEventCh <- duration
}

func (v *voteContext) emitWatcherCloseEvent() {
	close(v.watcherCloseEventCh)
}

func (v *voteContext) stop() {
	v.stopped = true
	v.stopTimer.Stop()
}

func (p *Processor) registerRequestedVote(voteCtx *voteContext) {
	p.requestedVoteMutex.Lock()
	defer p.requestedVoteMutex.Unlock()
	p.requestedVote[voteCtx.voteId] = voteCtx
}

func (p *Processor) unregisterRequestedVote(voteCtx *voteContext) {
	p.requestedVoteMutex.Lock()
	defer p.requestedVoteMutex.Unlock()
	delete(p.requestedVote, voteCtx.voteId)
}

func (p *Processor) getRequestedVote(voteId string) (*voteContext, bool) {
	p.requestedVoteMutex.Lock()
	defer p.requestedVoteMutex.Unlock()
	voteCtx, ok := p.requestedVote[voteId]
	return voteCtx, ok
}

func (p *Processor) popRequestedVote(voteId string) (*voteContext, bool) {
	p.requestedVoteMutex.Lock()
	defer p.requestedVoteMutex.Unlock()
	voteCtx, ok := p.requestedVote[voteId]
	if ok {
		delete(p.requestedVote, voteId)
	}
	return voteCtx, ok
}

func (p *Processor) createVoteId() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("can not create uuid: %w", err)
	}
	s := sha1.Sum([]byte(time.Now().String()))
	return fmt.Sprintf("%v-%x", u.String(), s), nil
}

func (p *Processor) createVoteContext(request *pb.OpenVoteRequest) (*voteContext, error) {
	voteId, err:= p.createVoteId()
	if err != nil {
		return nil, fmt.Errorf("can not create voteId: %w", err)
	}
	counts := make([]*pb.VoteCount, len(request.Choices))
	for i := 0; i < len(request.Choices); i += 1 {
		request.Choices[i].Label = norm.NFKC.String(request.Choices[i].Label)
		counts[i] = &pb.VoteCount {
			Label: request.Choices[i].Label,
			Choice: request.Choices[i].Choice,
			Count: 0,
		}
	}
	voteCtx := &voteContext {
		voteId: voteId,
		videoId: request.VideoId,
		target: request.Target,
		duration: request.Duration,
		choices: request.Choices,
		stopTimer: nil,
		watcherResetEventCh: make(chan int32),
		watcherCloseEventCh: make(chan int),
		total: 0,
		counts: counts,
		voted: make(map[string]bool),
		stopped: false,
	}
	return voteCtx, nil
}

type match struct {
	choiceIdx  int
	messageIdx int
}

func (p *Processor) voteWatcher(voteCtx *voteContext) {
	if p.verbose {
		log.Printf("vote watch start (voteId = %v)", voteCtx.voteId)
	}
	subscribeActiveLiveChatParams, err := p.collector.SubscribeActiveLiveChat(voteCtx.videoId)
	if err != nil {
		p.unregisterRequestedVote(voteCtx)
		if p.verbose {
			log.Printf("can not subsctibe (voteId = %v)", voteCtx.voteId)
		}
		return
	}
	defer p.collector.UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
	voteCtx.setStopTimer()
	for {
		select {
		case response, ok := <-subscribeActiveLiveChatParams.GetSubscriberCh():
			if !ok {
				p.unregisterRequestedVote(voteCtx)
				voteCtx.stop()
				if p.verbose {
					log.Printf("vote watch end (voteId = %v)", voteCtx.voteId)
				}
				return
			}
			if voteCtx.stopped {
				// expired and not be counted
				break
			}
			for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
				if activeLiveChatMessage.DisplayMessage == "" {
					continue
				}
				_, ok := voteCtx.voted[activeLiveChatMessage.AuthorChannelId]
				if ok {
					// already voted
					continue
				}
				if voteCtx.target == pb.Target_OWNER_MODERATOR {
					if !(activeLiveChatMessage.AuthorIsChatModerator ||
						activeLiveChatMessage.AuthorIsChatOwner) {
						continue
					}
				} else if voteCtx.target == pb.Target_OWNER_MODERATOR_SPONSOR {
					if !(activeLiveChatMessage.AuthorIsChatModerator ||
						activeLiveChatMessage.AuthorIsChatOwner ||
						activeLiveChatMessage.AuthorIsChatSponsor) {
						continue
					}
				}
				normDisplayMessage := norm.NFKC.String(activeLiveChatMessage.DisplayMessage)
				normDisplayMessage = p.stampRe.ReplaceAllString(normDisplayMessage, "")

				matches := make([]*match, 0, len(voteCtx.choices))
				for choiceIdx := 0; choiceIdx < len(voteCtx.choices); choiceIdx += 1 {
					choice := voteCtx.choices[choiceIdx]
					messageIdx := strings.Index(normDisplayMessage, choice.Label)
					if messageIdx == -1 {
						continue
					}
					matches = append(matches, &match{ choiceIdx: choiceIdx, messageIdx: messageIdx })
				}
				if len(matches) == 0 {
					// not match label
					continue
				}
				sort.Slice(matches , func(i, j int) bool { return matches[i].messageIdx < matches[j].messageIdx })
				m := matches[0]
				voteCtx.total += 1
				voteCtx.counts[m.choiceIdx].Count += 1
				voteCtx.voted[activeLiveChatMessage.AuthorChannelId] = true
			}
		case duration := <-voteCtx.watcherResetEventCh:
			if p.verbose {
				log.Printf("reset event (voteId = %v)", voteCtx.voteId)
			}
			voteCtx.resetStopTimer(duration)
		case <-voteCtx.watcherCloseEventCh:
			if p.verbose {
				log.Printf("close event (voteId = %v)", voteCtx.voteId)
			}
			voteCtx.stop()
			if p.verbose {
				log.Printf("vote watch end (voteId = %v)", voteCtx.voteId)
			}
			return
		case <-voteCtx.stopTimer.C:
			if p.verbose {
				log.Printf("stop timer (voteId = %v)", voteCtx.voteId)
			}
			voteCtx.stop()
		}
	}
}

func (p *Processor) OpenVote(request *pb.OpenVoteRequest) (*pb.OpenVoteResponse, error) {
	status := new(pb.Status)
	voteCtx, err := p.createVoteContext(request)
	if err != nil {
                status.Code = pb.Code_INTERNAL_ERROR
                status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		return &pb.OpenVoteResponse{
			Status: status,
			VoteId: "",
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
		return &pb.OpenVoteResponse{
			Status: status,
			VoteId: "",
			Video: nil,
		}, nil
	}
	if startCollectionActiveLiveChatResponse.Status.Code != pb.Code_SUCCESS && startCollectionActiveLiveChatResponse.Status.Code != pb.Code_IN_PROGRESS {
		return &pb.OpenVoteResponse{
			Status: startCollectionActiveLiveChatResponse.Status,
			VoteId: "",
			Video: nil,
		}, nil
	}
	// XXX if startCollectionActiveLiveChatResponse.Status.Code == pb.Code_IN_PROGRESS: getVideo
	p.registerRequestedVote(voteCtx)
	go p.voteWatcher(voteCtx)
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v, voteId = %v)", request.VideoId, voteCtx.voteId)
        return &pb.OpenVoteResponse{
                Status: status,
		VoteId: voteCtx.voteId,
                Video:  startCollectionActiveLiveChatResponse.Video,
        }, nil
}

func (p *Processor) UpdateVoteDuration(request *pb.UpdateVoteDurationRequest) (*pb.UpdateVoteDurationResponse, error) {
	status := new(pb.Status)
	voteCtx, ok := p.getRequestedVote(request.VoteId)
	if !ok {
                status.Code = pb.Code_NOT_FOUND
                status.Message = fmt.Sprintf("not found vote context (voteId = %v)", request.VoteId)
		return &pb.UpdateVoteDurationResponse{
			Status: status,
		}, nil
	}
	voteCtx.emitWatcherResetEvent(request.Duration)
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v, voteId = %v)", voteCtx.videoId, voteCtx.voteId)
        return &pb.UpdateVoteDurationResponse{
                Status: status,
        }, nil
}

func (p *Processor) GetVoteResult(request *pb.GetVoteResultRequest) (*pb.GetVoteResultResponse, error) {
	status := new(pb.Status)
	voteCtx, ok := p.getRequestedVote(request.VoteId)
	if !ok {
                status.Code = pb.Code_NOT_FOUND
                status.Message = fmt.Sprintf("not found vote context (voteId = %v)", request.VoteId)
		return &pb.GetVoteResultResponse{
			Status: status,
		}, nil
	}
	if p.verbose {
		log.Printf("total = %v, counts = %v", voteCtx.total, voteCtx.counts)
	}
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v, voteId = %v)", voteCtx.videoId, voteCtx.voteId)
        return &pb.GetVoteResultResponse{
                Status: status,
		Total: voteCtx.total,
		Counts: voteCtx.counts,
        }, nil
}

func (p *Processor) CloseVote(request *pb.CloseVoteRequest) (*pb.CloseVoteResponse, error) {
	status := new(pb.Status)
	voteCtx, ok := p.popRequestedVote(request.VoteId)
	if !ok {
                status.Code = pb.Code_NOT_FOUND
                status.Message = fmt.Sprintf("not found vote context (voteId = %v)", request.VoteId)
		return &pb.CloseVoteResponse{
			Status: status,
		}, nil
	}
	voteCtx.emitWatcherCloseEvent()
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v, voteId = %v)", voteCtx.videoId, voteCtx.voteId)
        return &pb.CloseVoteResponse{
                Status: status,
        }, nil
}


type groupingContext struct {
	groupingId                          string
	videoId                             string
	target                              pb.Target
	choices                             []*pb.GroupingChoice
	group                               map[string]int
	watcherCloseEventCh                 chan int
	subscriberCh                        chan *pb.PollGroupingActiveLiveChatResponse
}

func (g *groupingContext) emitWatcherCloseEvent() {
	close(g.watcherCloseEventCh)
}

func (g *groupingContext) GetSubscriberCh() chan *pb.PollGroupingActiveLiveChatResponse {
        return g.subscriberCh
}

func (p *Processor) registerRequestedGrouping(groupingCtx *groupingContext) {
	p.requestedGroupingMutex.Lock()
	defer p.requestedGroupingMutex.Unlock()
	_, ok := p.requestedGrouping[groupingCtx.groupingId]
	if ok {
		log.Printf("already exists groupingId (groupingId = %v)", groupingCtx.groupingId)
		return
	}
	p.requestedGrouping[groupingCtx.groupingId] = groupingCtx
}

func (p *Processor) unregisterRequestedGrouping(groupingCtx *groupingContext) {
	p.requestedGroupingMutex.Lock()
	defer p.requestedGroupingMutex.Unlock()
	_, ok := p.requestedGrouping[groupingCtx.groupingId]
	if !ok {
		log.Printf("not found groupingId (groupingId = %v)", groupingCtx.groupingId)
		return
	}
        delete(p.requestedGrouping, groupingCtx.groupingId)
}

func (p *Processor) getRequestedGrouping(groupingId string) (*groupingContext, bool) {
	p.requestedGroupingMutex.Lock()
	defer p.requestedGroupingMutex.Unlock()
	groupingCtx, ok := p.requestedGrouping[groupingId]
	return groupingCtx, ok
}

func (p *Processor) popRequestedGrouping(groupingId string) (*groupingContext, bool) {
        p.requestedGroupingMutex.Lock()
        defer p.requestedGroupingMutex.Unlock()
        groupingCtx, ok := p.requestedGrouping[groupingId]
        if !ok {
		log.Printf("not found groupingId (groupingId = %v)", groupingId)
		return groupingCtx, ok
	}
        delete(p.requestedGrouping, groupingId)
        return groupingCtx, ok
}

func (p *Processor) createGroupingId() (string, error) {
	u, err := uuid.NewRandom()
	if err != nil {
		return "", fmt.Errorf("can not create uuid: %w", err)
	}
	s := sha1.Sum([]byte(time.Now().String()))
	return fmt.Sprintf("%v-%x", u.String(), s), nil
}

func (p *Processor) createGroupingContext(request *pb.StartGroupingActiveLiveChatRequest) (*groupingContext, error) {
	groupingId, err:= p.createGroupingId()
	if err != nil {
		return nil, fmt.Errorf("can not create grouping id: %w", err)
	}
	for i := 0; i < len(request.Choices); i += 1 {
		request.Choices[i].Label = norm.NFKC.String(request.Choices[i].Label)
	}
	groupingCtx := &groupingContext {
		groupingId:                       groupingId,
		videoId:                          request.VideoId,
		target:                           request.Target,
		choices:                          request.Choices,
		group:                            make(map[string]int),
		watcherCloseEventCh:              make(chan int),
		subscriberCh:                     make(chan *pb.PollGroupingActiveLiveChatResponse),
	}
	return groupingCtx, nil
}

func (p *Processor) groupingWatcher(groupingCtx *groupingContext) {
	if p.verbose {
		log.Printf("start grouping watch (groupingId = %v)", groupingCtx.groupingId)
	}
	subscribeActiveLiveChatParams, err := p.collector.SubscribeActiveLiveChat(groupingCtx.videoId)
	if err != nil {
		p.unregisterRequestedGrouping(groupingCtx)
		if p.verbose {
			log.Printf("can not subsctibe (groupingId = %v)", groupingCtx.groupingId)
		}
		return
	}
	defer p.collector.UnsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
	for {
		select {
		case response, ok := <-subscribeActiveLiveChatParams.GetSubscriberCh():
			if !ok {
				// groupingContextの登録を解除してからハンドラののチャンネル読み出しを終了させる
				p.unregisterRequestedGrouping(groupingCtx)
				close(groupingCtx.subscriberCh)
				if p.verbose {
					log.Printf("end grouping watch (groupingId = %v)", groupingCtx.groupingId)
				}
				return
			}
			for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
				if activeLiveChatMessage.DisplayMessage == "" {
					continue
				}
				groupIdx, ok := groupingCtx.group[activeLiveChatMessage.AuthorChannelId]
				if ok {
					// already grouping

					// leave group
					normDisplayMessage := norm.NFKC.String(activeLiveChatMessage.DisplayMessage)
					normDisplayMessage = p.stampRe.ReplaceAllString(normDisplayMessage, "")
					messageIdx := strings.Index(normDisplayMessage, ";;;")
					if !(messageIdx == -1)  {
						if p.verbose {
							log.Printf("leave group (id = %v, index = %v)", activeLiveChatMessage.AuthorChannelId, groupIdx)
						}
						delete(groupingCtx.group, activeLiveChatMessage.AuthorChannelId)
					}

                                        groupingCtx.subscriberCh <- &pb.PollGroupingActiveLiveChatResponse{
						Status: &pb.Status{
							Code:    pb.Code_SUCCESS,
							Message: fmt.Sprintf("success (groupingId = %v)", groupingCtx.groupingId),
						},
						GroupingActiveLiveChatMessage: &pb.GroupingActiveLiveChatMessage {
                                                        GroupIdx: int32(groupIdx),
                                                        Label: groupingCtx.choices[groupIdx].Label,
                                                        Choice: groupingCtx.choices[groupIdx].Choice,
                                                        ActiveLiveChatMessage: activeLiveChatMessage,
                                                },
					}
					continue
				}
				if groupingCtx.target == pb.Target_OWNER_MODERATOR {
					if !(activeLiveChatMessage.AuthorIsChatModerator ||
						activeLiveChatMessage.AuthorIsChatOwner) {
						continue
					}
				} else if groupingCtx.target == pb.Target_OWNER_MODERATOR_SPONSOR {
					if !(activeLiveChatMessage.AuthorIsChatModerator ||
						activeLiveChatMessage.AuthorIsChatOwner ||
						activeLiveChatMessage.AuthorIsChatSponsor) {
						continue
					}
				}
				normDisplayMessage := norm.NFKC.String(activeLiveChatMessage.DisplayMessage)
				normDisplayMessage = p.stampRe.ReplaceAllString(normDisplayMessage, "")
				matches := make([]*match, 0, len(groupingCtx.choices))
				for choiceIdx := 0; choiceIdx < len(groupingCtx.choices); choiceIdx += 1 {
					choice := groupingCtx.choices[choiceIdx]
					messageIdx := strings.Index(normDisplayMessage, choice.Label)
					if messageIdx == -1 {
						continue
					}
					matches = append(matches, &match{ choiceIdx: choiceIdx, messageIdx: messageIdx })
				}
				if len(matches) == 0 {
					// not match label
					continue
				}
				// new grouping
				sort.Slice(matches , func(i, j int) bool { return matches[i].messageIdx < matches[j].messageIdx })
				m := matches[0]
				groupIdx = m.choiceIdx
				groupingCtx.group[activeLiveChatMessage.AuthorChannelId] = groupIdx
				if p.verbose {
					log.Printf("join group (id = %v, index = %v)", activeLiveChatMessage.AuthorChannelId, groupIdx)
				}
				groupingCtx.subscriberCh <- &pb.PollGroupingActiveLiveChatResponse{
					Status: &pb.Status{
						Code:    pb.Code_SUCCESS,
						Message: fmt.Sprintf("success (groupingId = %v)", groupingCtx.groupingId),
					},
					GroupingActiveLiveChatMessage: &pb.GroupingActiveLiveChatMessage {
                                                GroupIdx: int32(groupIdx),
                                                Label: groupingCtx.choices[groupIdx].Label,
						Choice: groupingCtx.choices[groupIdx].Choice,
						ActiveLiveChatMessage: activeLiveChatMessage,
					},
				}
			}
		case <-groupingCtx.watcherCloseEventCh:
			close(groupingCtx.subscriberCh)
			if p.verbose {
				log.Printf("end grouping watch (groupingId = %v)", groupingCtx.groupingId)
			}
			return
		}
	}
}

func (p *Processor) StartGroupingActiveLiveChat(request *pb.StartGroupingActiveLiveChatRequest) (*pb.StartGroupingActiveLiveChatResponse, error) {
	status := new(pb.Status)
	groupingCtx, err := p.createGroupingContext(request)
	if err != nil {
                status.Code = pb.Code_INTERNAL_ERROR
                status.Message = fmt.Sprintf("%v (videoId = %v)", err, request.VideoId)
		return &pb.StartGroupingActiveLiveChatResponse{
			Status: status,
			GroupingId: "",
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
		return &pb.StartGroupingActiveLiveChatResponse{
			Status: status,
			GroupingId: "",
			Video: nil,
		}, nil
	}
	if startCollectionActiveLiveChatResponse.Status.Code != pb.Code_SUCCESS && startCollectionActiveLiveChatResponse.Status.Code != pb.Code_IN_PROGRESS {
		return &pb.StartGroupingActiveLiveChatResponse{
			Status: startCollectionActiveLiveChatResponse.Status,
			GroupingId: "",
			Video: nil,
		}, nil
	}
	// XXX if startCollectionActiveLiveChatResponse.Status.Code == pb.Code_IN_PROGRESS: getVideo
	// groupingContextを登録してwatcherを開始
	p.registerRequestedGrouping(groupingCtx)
	go p.groupingWatcher(groupingCtx)
	status.Code = pb.Code_SUCCESS
        status.Message = fmt.Sprintf("success (videoId = %v, groupingId = %v)", request.VideoId, groupingCtx.groupingId)
        return &pb.StartGroupingActiveLiveChatResponse{
                Status: status,
		GroupingId: groupingCtx.groupingId,
                Video:  startCollectionActiveLiveChatResponse.Video,
        }, nil
}

func (p *Processor) SubscribeGroupingActiveLiveChat(groupingId string) (*groupingContext, error) {
	groupingCtx, ok := p.getRequestedGrouping(groupingId)
	if !ok {
		return nil, fmt.Errorf("not found groupingId (groupingId = %v)", groupingId)
	}
	return groupingCtx, nil
}

func (p *Processor) UnsubscribeGroupingActiveLiveChat(groupingId string) {
	// watcherがすでに登録解除している場合は何もしない
	groupingCtx, ok := p.popRequestedGrouping(groupingId)
	if !ok {
		if p.verbose {
			log.Printf("not found groupingId (groupingId = %v)", groupingId)
		}
		return
	}
        go p.discardGroupingActiveLiveChatUntilClosed(groupingCtx)
	groupingCtx.emitWatcherCloseEvent()
}

func (p *Processor) discardGroupingActiveLiveChatUntilClosed(groupingCtx *groupingContext) {
        // This is workaround of publisher blocking in case client closing
        // XXX refactor
        for {
                select {
                case _, ok := <-groupingCtx.GetSubscriberCh():
                        if !ok {
                                return
                        }
                }
        }
}

func NewProcessor(collector *collector.Collector, mecabrc string, font string, opts ...Option) *Processor {
	baseOpts := defaultOptions()
	for _, opt := range opts {
		if opt == nil {
                        continue
                }
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
		requestedVoteMutex:           new(sync.Mutex),
		requestedVote:                make(map[string]*voteContext),
		requestedGroupingMutex:       new(sync.Mutex),
		requestedGrouping:            make(map[string]*groupingContext),
		stampRe:                      regexp.MustCompile(`:[^:]+?:`),

	}
}
