package collector

import (
	"fmt"
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
	prevUrl string
	url     string
}

func newArchiveLiveChatMessagesParams(url string) (*archiveLiveChatMessagesParams) {
	return &archiveLiveChatMessagesParams {
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
		return nil, fmt.Errorf("can not create http request (url = %v): %w", url, err)
	}
	if useUserAgent {
		req.Header.Set("User-Agent", userAgent)
	}
	client := new(http.Client)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("can not request of http (url = %v): %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("response have unexpected status (url = %v, status = %v)", url, resp.Status)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("can not read body (url = %v): %w", url, err)
	}
	return body, nil
}

func (c *Collector) getArchiveLiveChatFirstLiveChatReplayUrl(videoId string) (string, error) {
	url := youtubeBaseUrl + videoId
        body, err := c.getPage(url, false)
        if err != nil {
		return "", fmt.Errorf("can not get video page (url = %v): %w", url, err)
        }
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("can not parse body (url = %v): %w", url, err)
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
		return "", fmt.Errorf("can not get video page (url = %v): %w", url, err)
        }
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("can not parse body (url = %v): %w", url, err)
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
		return "", fmt.Errorf("not found ytInitialData (url = %v): %w", url, err)
	}
	return yuInitialDataStr, nil
}

func (c *Collector) getArchiveLiveChatMessages(channelId string, videoId string, params *archiveLiveChatMessagesParams)(error) {
	archiveLiveChatMessages := make([]*pb.ArchiveLiveChatMessage, 0, 2000)
	var yuInitialDataStr string
	yuInitialDataStr, err := c.getArchiveLiveChatYtInitialData(params.getUrl())
	if err != nil {
		return fmt.Errorf("can not get ytInitialData (url = %v, videoId = %v): %w", params.getUrl(), videoId, err)
	}
	if yuInitialDataStr == "" {
		return fmt.Errorf ("not found ytInitialData (url = %v, videoId = %v)", params.getUrl(), videoId)
	}
	var ytInitialData YtInitialData
	if err := json.Unmarshal([]byte(yuInitialDataStr), &ytInitialData); err != nil {
		return fmt.Errorf("can not unmarshal ytInitialData (url = %v, yuInitialDataStr = %v, videoId = %v): %w", params.getUrl(), yuInitialDataStr, videoId, err)
	}
	var nextId string
	if len(ytInitialData.ContinuationContents.LiveChatContinuation.Continuations) >= 2 {
		nextId = ytInitialData.ContinuationContents.LiveChatContinuation.Continuations[0].LiveChatReplayContinuationData.Continuation
	}
	if c.verbose {
		log.Printf("nextId = %v, videoId = %v", nextId, videoId)
	}
	for _, liveChatContinuationAction := range ytInitialData.ContinuationContents.LiveChatContinuation.Actions {
		videoOffsetTimeMsec := liveChatContinuationAction.ReplayChatItemAction.VideoOffsetTimeMsec
		for _, replayChatItemAction := range liveChatContinuationAction.ReplayChatItemAction.Actions {
			clientId := replayChatItemAction.AddChatItemAction.ClientID
			if replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampText.SimpleText != "" {
				timestampText := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampText.SimpleText
				authorName := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.AuthorName.SimpleText
				id := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.ID
				var messageText string
				for _, run := range replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.Message.Runs {
					messageText += run.Text
				}
				purchaseAmountText := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.PurchaseAmountText.SimpleText
				timestampUsec := replayChatItemAction.AddChatItemAction.Item.LiveChatPaidMessageRenderer.TimestampUsec
				archiveLiveChatMessage := &pb.ArchiveLiveChatMessage{
					MessageId: id,
					ChannelId: channelId,
					VideoId: videoId,
					TimestampUsec: timestampUsec,
					ClientId: clientId,
					AuthorName: authorName,
					MessageText: messageText,
					PurchaseAmountText: purchaseAmountText,
					VideoOffsetTimeMsec: videoOffsetTimeMsec,
				}
				archiveLiveChatMessages = append(archiveLiveChatMessages, archiveLiveChatMessage)
			} else if replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampText.SimpleText != "" {
				timestampText := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampText.SimpleText
				authorName := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.AuthorName.SimpleText
				id := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.ID
				var messageText string
				for _, run := range replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.Message.Runs {
					messageText += run.Text
				}
				timestampUsec := replayChatItemAction.AddChatItemAction.Item.LiveChatTextMessageRenderer.TimestampUsec
				archiveLiveChatMessage := &pb.ArchiveLiveChatMessage{
					MessageId: id,
					ChannelId: channelId,
					VideoId: videoId,
					TimestampUsec: timestampUsec,
					ClientId: clientId,
					AuthorName: authorName,
					MessageText: messageText,
					PurchaseAmountText: "",
					VideoOffsetTimeMsec: videoOffsetTimeMsec,
				}
				archiveLiveChatMessages = append(archiveLiveChatMessages, archiveLiveChatMessage)
			}
		}
	}
	if err := c.dbOperator.UpdateArchiveLiveChatMessages(params.getPrevUrl(), params.getUrl(), archiveLiveChatMessages); err != nil {
                return fmt.Errorf("can not update archhive live chat in database (videoId = %v): %w", videoId, err)
        }
	params.updateUrl(nextId)
	return nil
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
        firstLiveChatReplayUrl, err := c.getArchiveLiveChatFirstLiveChatReplayUrl(videoId)
        if err != nil {
                c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                log.Printf ("can not get first live chat replay url (videoId = %v): %w", videoId, err)
                return
        }
        if c.verbose {
                log.Printf("first live chat replay url = %v", firstLiveChatReplayUrl)
        }
        if firstLiveChatReplayUrl == "" {
                if c.verbose {
                        log.Printf("skip collect archive live chat because can not get first live chat replay url")
                }
                c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                return
        }
        params := newArchiveLiveChatMessagesParams(firstLiveChatReplayUrl)
        for {
                retry := 0
                for {
                        if err := c.getArchiveLiveChatMessages(channelId, videoId, params); err != nil {
                                retry += 1
                                log.Printf("can not get live chat (videoId = %v, nextUrl = %v), retry ...: %v", videoId, params.getUrl(), err)
                                time.Sleep(time.Second)
                                if retry > retryMax {
                                        c.unregisterRequestedVideoForArchiveLiveChat(videoId)
                                        log.Printf("... giveup, can not get live chat (videoId = %v, nextUrl = %v): %v", videoId, params.getUrl(), err)
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
