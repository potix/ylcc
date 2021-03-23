package collector

import (
	"time"
	"encoding/json"
	"strings"
	"log"
	"bytes"
	"net/http"
	"io/ioutil"
	"github.com/PuerkitoBio/goquery"
	 pb "github.com/potix/ylcc/protocol"
)

const(
	youtubeBaseUrl string = "https://www.youtube.com/watch?v="
	youtubeLiveChatBaseUrl string = "https://www.youtube.com/live_chat_replay?continuation="
	userAgent string = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:70.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36 Gecko/20100101 Firefox/70.0"
	maxRetry int    = 10
)

type archiveLiveChatMessagesParams struct {
	last    bool
	url     string
	nextUrl string
}

func newArchiveLiveChatMessagesParams(url string) (*archiveLiveChatMessagesParams) {
	return &liveChatMessagesParams {
		last: false,
		prevUrl: "",
		url: url,
	}
}

func (l *archiveLiveChatMessagesParams) updateUrl(id string) {
	l.prevUrl = l.url
	if id == "" {
		l.url = ""
		return
	} else {
		l.url = youtubeLiveChatBaseUrl + id
	}
}

func (l *archiveLiveChatMessagesParams) getPrevUrl() (string) {
	return l.prevUrl
}

func (l *archiveLiveChatMessagesParams) getUrl() (string) {
	return l.url
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

func (c *Collector) getArchiveLiveChatFirstLiveChatReplayUrl() (string, error) {
	url := youtubeBaseUrl + l.videoId
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

func (c *Collector) getArchiveLiveChatYtInitialData(url string)(string, error) {
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

func (c *Collector)getArchiveLiveChatMessage(channelId string, videoId string, liveChatContinuationAction string) (*pb.ArchiveLiveChatMessage) {
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
				UniqueId: videoId + ".paid." + id + "." + timestampUsec + "." + clientId,
				ChannelId: channelId,
				VideoId: videoId,
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
				UniqueId: videoId + ".text." + id + "." + timestampUsec + "." + clientId,
				ChannelId: channelId,
				VideoId: videoId,
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

func (c *Collector) getArchiveLiveChatMessages(channelId string, videoId string, params *archiveLiveChatMessagesParams)(error) {
	archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0, 2000)
	var yuInitialDataStr string
	yuInitialDataStr, err := c.getArchiveLiveChatYtInitialData(youtubeLiveChatBaseUrl + params.getId())
	if err != nil {
		return errors.Wrapf(err, "can not get ytInitialData (url = %v, videoId = %v)", url, videoId)
	}
	if yuInitialDataStr == "" {
		return errors.Errorf("not found ytInitialData (url = %v, videoId = %v)", url, videoId)
	}
	var ytInitialData YtInitialData
	if err := json.Unmarshal([]byte(yuInitialDataStr), &ytInitialData); err != nil {
		return errors.Wrapf(err, "can not unmarshal ytInitialData (url = %v, yuInitialDataStr = %v, videoId = %v)", url, yuInitialDataStr, videoId)
	}
	var nextId string
	if len(ytInitialData.ContinuationContents.LiveChatContinuation.Continuations) >= 2 {
		nextId = ytInitialData.ContinuationContents.LiveChatContinuation.Continuations[0].LiveChatReplayContinuationData.Continuation
	}
	if c.verbose {
		log.Printf("nextId = %v, videoId = %v", nextId, videoId)
	}
	for _, liveChatContinuationAction := range ytInitialData.ContinuationContents.LiveChatContinuation.Actions {
		append(archiveLiveChatMessages, c.getLiveChatMessage(channelId, videoId, liveChatContinuationAction))
	}
	if err := c.dbOperator.UpdateArchiveLiveChatMessages(params.getPrevId(), params.getId(), archiveLiveChatMessages); err != nil {
                log.Printf("can not update live chat (videoId = %v): %v", videoId, err)
                return
        }
	params.updateId(nextId)
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
        firstLiveChatReplayUrl, err := c.getArchiveLiveChatFirstLiveChatReplayUrl()
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
                        if err := c.getArchiveLiveChatMessages(params, channelId, videoId); err != nil {
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
                if params.getUrl() == "" {
                        break
                }
        }
        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
        return
}
