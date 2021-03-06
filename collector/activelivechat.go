package collector

import (
    "time"
    "fmt"
    "context"
    "google.golang.org/api/option"
    "google.golang.org/api/transport/http"
    "google.golang.org/api/youtube/v3"

)

func main() {
	messages :=  make([]string, 0, storeMsgMax)
	apiKey := "AIzaSyA9PRuWuml7_YsBZC12v5BK9HgdhgbknAc"
	ctx := context.Background()
	newClient, _, err := http.NewClient(ctx, option.WithAPIKey(apiKey))
	if err != nil {
		fmt.Printf("can not create client: %v\n", err)
		return
	}
	youtubeService, err := youtube.New(newClient)
	if err != nil {
		fmt.Printf("can not create youtube service: %v\n", err)
		return
	}
	videosListCall := youtubeService.Videos.List([]string{"snippet", "liveStreamingDetails"})
	videosListCall.Id("5VoIGGMYrDg")
	videoListResponse, err := videosListCall.Do()
	if err != nil {
		fmt.Printf("can not get videos: %v\n", err)
		return
	}
	if len(videoListResponse.Items) < 1 {
		fmt.Printf("not found video\n")
		return
	}
	if videoListResponse.Items[0].LiveStreamingDetails.ActiveLiveChatId == "" {
		fmt.Printf("not found active live chat Id\n")
		return
	}
	activeLiveChatID := videoListResponse.Items[0].LiveStreamingDetails.ActiveLiveChatId
	fmt.Printf("active live chat id: %v\n", activeLiveChatID)
	nextPageToken := ""
	for {
		liveChatMessagesListCall := youtubeService.LiveChatMessages.List(activeLiveChatID, []string{"snippet", "authorDetails"})
		liveChatMessagesListCall.PageToken(nextPageToken)
		liveChatMessagesListCall.MaxResults(2000)
		liveChatMessageListResponse, err := liveChatMessagesListCall.Do()
		if err != nil {
			fmt.Printf("can not get live chat messages: %v\n", err)
			return
		}
		etag = liveChatMessageListResponse.Etag
		nextPageToken = liveChatMessageListResponse.NextPageToken
		pollingIntervalMillis := liveChatMessageListResponse.PollingIntervalMillis
		fmt.Printf("next page token %v\n", nextPageToken)
		fmt.Printf("polling interval millis %v\n", pollingIntervalMillis)
		for _, item := range liveChatMessageListResponse.Items {
			messageId := item.Id
			authorChannelId := item.Snippet.AuthorChannelId
			liveChatId := item.Snippet.LiveChatId
			if item.Snippet.SuperChatDetails != nil {
				amountDisplayString = item.Snippet.SuperChatDetails.AmountDisplayString
				Currency = item.Snippet.SuperChatDetails.AmountDisplayString
				messages = append(messages, item.Snippet.SuperChatDetails.UserComment)
			} else if item.Snippet.TextMessageDetails != nil {
				messages = append(messages, item.Snippet.TextMessageDetails.MessageText)
			}
		}
		if len(messages) > storeMsgMax {
			messages = messages[len(messages) - analyzeMsgMax:]
		}
		anyalizeMessages := messages
		if len(anyalizeMessages) > analyzeMsgMax {
			anyalizeMessages = anyalizeMessages[len(anyalizeMessages) - analyzeMsgMax:]
		}
		fmt.Printf("=====\n")
		for _, msg := range anyalizeMessages {
			fmt.Printf("%v\n", msg)
		}
		time.Sleep(time.Duration(pollingIntervalMillis) * time.Millisecond)
	}

}



