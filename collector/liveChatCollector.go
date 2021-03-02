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

type liveChatMessagesContext {
	last bool
	url string
	liveChatMessages []*database.LiveChatMessage
}

func newLiveChatMessagesContext(nextUrl string) (*liveChatMessagesContext) {
	return &liveChatMessagesContext {
		last: false,
		url: url,
		liveChatMessages: make([]*database.LiveChatMessage, 0, 2000)
	}
}

func (l *liveChatMessagesContext) updateUrl(url string) {
	l.url = url
}

func (l *liveChatMessagesContext) getUrl() string) {
	return l.url
}

func (l *liveChatMessagesContext) appendLiveChatMessage(liveChatMessages *database.LiveChatMessage) () {
	l.liveChatMessages =  append(l.liveChatMessages, liveChatMessage)
}

func (l *liveChatMessagesContext) getLiveChatMessages() ([]*database.LiveChatMessage) {
	return l.liveChatMessages
}

func (l *liveChatMessagesContext) setLast() {
	l.last = true
}

func (l *liveChatMessagesContext) isLast() {
	return l.last == true
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

func (l *LiveCharCollector)updateLiveChatMessage(liveChatContinuationAction string) (*datanase.LiveChatMessage) {
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

func (c *Collector)getLiveChatMessages(ctx *liveChatMessagesContext)(error) {
	var yuInitialDataStr string
	yuInitialDataStr, err := c.getYtInitialData(ctx.getUrl())
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
		ctx.appendLiveChatMessage(c.getLiveChatMessage(liveChatContinuationAction))
	}
	if nextId == "" {
		ctx.setLast()
		return nil
	}
	ctx.updateUrl("https://www.youtube.com/live_chat_replay?continuation=" + nextId)
	return nil
}

func (c *Collector)CollectLiveChat(videoId) (error) {
	var liveChatMessage make([]*database.LiveChatMessage, 0, 2000),
	count, err := l.databaseOperator.CountLiveChatMessagesByVideoId(videoId)
	if err != nil {
		return errors.Wrapf(err, "can not get live chat from database (videoId = %v)", videoId)
	}
	if count> 0 {
		if l.verbose {
			log.Printf("already exists live chat in database (videoId = %v)", videoId)
		}
		return nil
	}
	firstLiveChatReplayUrl, err := l.getFirstLiveChatReplayUrl()
	if err != nil {
		return fmt.Errorf("can not get first live chat replay url (videoId = %v): %w", videoId, err)
	}
	if l.verbose {
		log.Printf("first live chat replay url = %v", firstLiveChatReplayUrl)
	}
	if firstLiveChatReplayUrl == "" {
		if l.verbose {
			log.Printf("skip collect live chat because can not get first live chat replay url")
		}
		return nil
	}
	ctx := newLiveChatMessagesContext(firstLiveChatReplayUrl)
	for {
		retry := 0
		for {
			err := l.getLiveChatMessages(ctx)
			if err != nil {
				retry += 1
				log.Printf("can not get live chat (videoId = %v, nextUrl = %v), retry ...: %v", l.videoId, nextUrl, err)
				time.Sleep(time.Second)
				if retry > maxRetry {
					return fmt.Errorf("... giveup, can not get live chat (videoId = %v, nextUrl = %v): %w", l.videoId, nextUrl, err)
				}
				continue
			}
			break;
		}
		if ctx.isLast() {
			break
		}
	}
	err = l.databaseOperator.UpdateLiveChatMessages(ctx.getLiveChatMessages())
	if err != nil {
		return fmt.Errorf("can not update live chat (videoId = %v): %w", l.videoId, err)
	}
	return nil
}
