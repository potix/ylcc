package collector


import (
	"time"
	"strconv"
	"encoding/json"
	"strings"
	"log"
	"context"
	"bytes"
	"net/http"
	"io/ioutil"
	"github.com/pkg/errors"
	"github.com/PuerkitoBio/goquery"
	"github.com/munoudesu/clipper/database"
)

const(
	userAgent string = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36 Gecko/20100101 Firefox/70.0"
	maxRetry = 10
)

type archiveLiveChatMessagesContext {
	first            bool
	firstUrl         string
	url              string
	nextUrl          string
	archiveLiveChatMessages []*pb.ArchiveLiveChatMessage
}

func newArchiveLiveChatMessagesContext(url string) (*archiveLiveChatMessagesContext) {
	return &liveChatMessagesContext {
		first: true,
		firstUrl: url,
		url: "",
		nextUrl: "",
		archiveLiveChatMessages: make([]*pb.ArchiveLiveChatMessage, 0, 2000)
	}
}

func (l *archiveLiveChatMessagesContext) updateNextUrl(url string) {
	l.fitst = false
	l.url = l.nextUrl
	l.nextUrl = url

}

func (l *archiveLiveChatMessagesContext) gettUrl() (string) {
	return l.url
}

func (l *archiveLiveChatMessagesContext) getNextUrl() (string) {
	if first {
		return l.firstUrl
	} else {
		return l.nextUrl
	}
}

func (l *archiveLiveChatMessagesContext) appendLiveChatMessage(archiveLiveChatMessage *pb.ArchiveLiveChatMessage) () {
	l.archiveLiveChatMessages =  append(l.archiveLiveChatMessages, archiveLiveChatMessage)
}

func (l *archiveLiveChatMessagesContext) getArchiveLiveChatMessages() ([]*pb.archiveLiveChatMessage) {
	return l.liveChatMessages
}


func (c *Collector) getPage(url string, useUserAgent bool) ([]byte, error) {
	if c.verbose {
		log.Printf("retrive url = %v", url)
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "can not create http request (url = %v)", url)
	}
	if useUserAgent {
		req.Header.Set("User-Agent", userAgent)
	}
	client := new(http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(err, "can not request of http (url = %v)", url)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.Errorf("response have unexpected status (url = %v, status = %v)", url, resp.Status)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.Wrapf(err, "can not read body (url = %v)", url)
	}
	return body, nil
}

func (c *Collector) getFirstLiveChatReplayUrl() (string, error) {
	url := "https://www.youtube.com/watch?v=" + l.videoId
        body, err := c.getPage(url, false)
        if err != nil {
                return "", errors.Wrapf(err, "can not get video page (url = %v)", url)
        }
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", errors.Wrapf(err, "can not parse body (url = %v)", url)
	}
	var firstLiveChatReplayUrl string
	doc.Find("#live-chat-iframe").Each(func(i int, s *goquery.Selection) {
		url, ok := s.Attr("src")
		if !ok {
			return
		}
		firstLiveChatReplayUrl = url
	})
	return firstLiveChatReplayUrl, nil
}

func (c *Collector) getYtInitialData(url string)(string, error) {
        body, err := c.getPage(url, true)
        if err != nil {
                return "", errors.Wrapf(err, "can not get video page (url = %v)", url)
        }
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", errors.Wrapf(err, "can not parse body (url = %v)", url)
	}
	var yuInitialDataStr string
	doc.Find("body>script").EachWithBreak(func(i int, s *goquery.Selection) (bool) {
		html := s.Text()
		if strings.Contains(html, "ytInitialData") {
			elems := strings.SplitN(html, "=", 2)
			if len(elems) < 2 {
				log.Printf("can not not parse ytInitialData (url = %v, html = %v)", url, html)
				return true
			}
			yuInitialDataStr = strings.TrimSuffix(strings.TrimSpace(elems[1]), ";")
			return false
		}
		return true
	})
	if yuInitialDataStr == "" {
		return "", errors.Wrapf(err, "not found ytInitialData (url = %v)", url)
	}
	return yuInitialDataStr, nil
}

func (l *LiveCharCollector)getArchiveLiveChatMessage(liveChatContinuationAction string) (*pb.ArchiveLiveChatMessage) {
	videoOffsetTimeMsec := liveChatContinuationAction.ReplayChatItemAction.VideoOffsetTimeMsec
	for _, replayChatItemAction := range liveChatContinuationAction.ReplayChatItemAction.Actions {
		clientId := replayChatItemAction.AddChatItemAction.ClientID
		if replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampText.SimpleText != "" {
			timestampText := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampText.SimpleText
			authorName := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.AuthorName.SimpleText
			var authorPhotoUrl string
			if len(replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.AuthorPhoto.Thumbnails) > 0 {
				authorPhotoUrl = replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.AuthorPhoto.Thumbnails[0].URL
			}
			id := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.ID
			var messageText string
			for _, run := range replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.Message.Runs {
				messageText += run.Text
			}
			purchaseAmountText := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.PurchaseAmountText.SimpleText
			timestampUsec := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampUsec
			return &database.LiveChatMessage{
				UniqueId: l.videoId + ".paid." + id + "." + timestampUsec + "." + clientId,
				ChannelId: l.channelId,
				VideoId: l.videoId,
				ClientId: clientId,
				MessageId: id,
				TimestampUsec: timestampUsec,
				AuthorName: authorName,
				AuthorPhotoUrl: authorPhotoUrl,
				MessageText: messageText,
				PurchaseAmountText: purchaseAmountText,
				VideoOffsetTimeMsec: videoOffsetTimeMsec,
			}
		} else if replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampText.SimpleText != "" {
			timestampText := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampText.SimpleText
			authorName := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.AuthorName.SimpleText
			var authorPhotoUrl string
			if len(replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.AuthorPhoto.Thumbnails) > 0 {
				authorPhotoUrl = replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.AuthorPhoto.Thumbnails[0].URL
			}
			id := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.ID
			var messageText string
			for _, run := range replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.Message.Runs {
				messageText += run.Text
			}
			timestampUsec := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampUsec
			return &database.LiveChatMessage{
				UniqueId: l.videoId + ".text." + id + "." + timestampUsec + "." + clientId,
				ChannelId: l.channelId,
				VideoId: l.videoId,
				ClientId: clientId,
				MessageId: id,
				TimestampUsec: timestampUsec,
				AuthorName: authorName,
				AuthorPhotoUrl: authorPhotoUrl,
				MessageText: messageText,
				PurchaseAmountText: "",
				VideoOffsetTimeMsec: videoOffsetTimeMsec,
			}
		}
	}
}

func (c *Collector) getArchiveLiveChatMessages(params *liveChatMessagesParams)(error) {
	var yuInitialDataStr string
	yuInitialDataStr, err := c.getYtInitialData(params.getNextUrl())
	if err != nil {
		return errors.Wrapf(err, "can not get ytInitialData (url = %v)", url)
	}
	if yuInitialDataStr == "" {
		return errors.Errorf("not found ytInitialData (url = %v)", url)
	}
	var ytInitialData YtInitialData
	err := json.Unmarshal([]byte(yuInitialDataStr), &ytInitialData)
	if err != nil {
		return errors.Wrapf(err, "can not unmarshal ytInitialData (url = %v, yuInitialDataStr = %v)", url, yuInitialDataStr)
	}
	var nextId string
	if len(ytInitialData.ContinuationContents.LiveChatContinuation.Continuations) >= 2 {
		nextId = ytInitialData.ContinuationContents.LiveChatContinuation.Continuations[0].LiveChatReplayContinuationData.Continuation
	}
	if l.verbose {
		log.Printf("nextId = %v", nextId)
	}
	for _, liveChatContinuationAction := range ytInitialData.ContinuationContents.LiveChatContinuation.Actions {
		params.appendArchiveLiveChatMessage(c.getLiveChatMessage(liveChatContinuationAction))
	}
	if nextId == "" {
		ctx.setLast()
		return nil
	}
	ctx.updateNextUrl("https://www.youtube.com/live_chat_replay?continuation=" + nextId)
	return nil
}

func (c *Collector) collectArchiveLiveChatFromYoutube(channelId string, videoId string, replace bool) {
        if !replace {
                count, err := c.dbOperator.CountArchiveLiveChatMessagesByVideoId(videoId)
                if err != nil {
                        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                        logPrintf("can not get archive live chat from database (videoId = %v): %v", videoId, err)
                        return
                }
                if count > 0 {
                        if l.verbose {
                                log.Printf("already exists archive live chat in database (videoId = %v)", videoId)
                        }
                        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                        return
                }
        }
        var liveChatMessage make([]*database.LiveChatMessage, 0, 2000),
        firstLiveChatReplayUrl, err := c.getFirstLiveChatReplayUrl()
        if err != nil {
                c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                log.Printf ("can not get first live chat replay url (videoId = %v): %w", videoId, err)
                return
        }
        if c.verbose {
                log.Printf("first live chat replay url = %v", firstLiveChatReplayUrl)
        }
        if firstLiveChatReplayUrl == "" {
                if l.verbose {
                        log.Printf("skip collect archive live chat because can not get first live chat replay url")
                }
                c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                return
        }
        params := newArchiveLiveChatMessagesParams(firstLiveChatReplayUrl)
        for {
                retry := 0
                for {
                        err := c.getArchiveLiveChatMessages(params)
                        if err != nil {
                                retry += 1
                                log.Printf("can not get live chat (videoId = %v, nextUrl = %v), retry ...: %v", l.videoId, nextUrl, err)
                                time.Sleep(time.Second)
                                if retry > retryMax {
                                        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                                        log.Printf("... giveup, can not get live chat (videoId = %v, nextUrl = %v): %v", l.videoId, nextUrl, err)
                                        return
                                }
                                continue
                        }
                        break;
                }
                if params.getNextUrl() == "" {
                        break
                }
        }
        err = c.dbOperator.UpdateArchiveLiveChatMessages(ctx.getToken, ctx.getNextToken, ctx.getLiveChatMessages())
        if err != nil {
                log.Printf("can not update live chat (videoId = %v): %v", l.videoId, err)
                c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                return
        }
        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
        return
}
