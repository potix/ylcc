package collector

import (
        "os"
        "strings"
        "io/ioutil"
)


type collector struct {
	verbose bool
	apiKey  string

}


// startCollectリクエストを受け取った
// リクエストを受け取ったらロックをかけてvideoId毎のprogressフラグを確認する
// videoId毎のprogressフラグが立っている場合はリクエストエラーを返す
// フラグが立ってない場合はフラグを立ててgoroutubeで処理を開始する
// ロックを解除
// goroutineで処理が終わったらロックしてprogress フラグを削除ロックを解除

// 
// dbのvideo情報をチェック
// dbのvideo情報にactiveLiveChatIdがあり、APIにもactiveLiveChatIdがある
// dbにvideo情報がある場合は
// dbにvideo情報がある場合は




func (h *handler) GetVideo(ctx context.Context, request *GetVideoRequest) (*GetVideoResponse, error) {
        return h.collector.GetVideo(request)
}

func (h *handler) StartCollectionActiveLiveChat(ctx context.Context, request *StartCollectionActiveLiveChatRequest) (*StartCollectionActiveLiveChatResponse, error) {
        return h.collector.StartCollectionActiveLiveChat(request)
}

func (h *handler) PollActiveLiveChat(request *PollActiveLiveChatRequest, server Ylcc_PollActiveLiveChatServer) (error)
{
        myCh := make(chan *PollActiveLiveChatResponse)
        h.collector.PollActiveLiveChatSubscribe(myCh, request)
        defer h.collector.PollActiveLiveChatUnsubscribe(myCh)
        for {
                select {
                case response <-myCh:
                        err := server.Send(response)
                        if err != nil {
                                return fmt.Errorf("can not send response: %w", err)
                        }
                default:
                        return fmt.Errorf("can not read channnel. probably closed channel.")
                }
        }
}

func (h *handler) GetCachedActiveLiveChat(ctx context.Context, server *GetCachedActiveLiveChatRequest) (*GetCachedActiveLiveChatResponse, error) {
        return h.collector.GetCachedActiveLiveChat(request)
}

func (h *handler) StartCollectionArchiveLiveChat(ctx context.Context, request *StartCollectionArchiveLiveChatRequest) (*StartCollectionArchiveLiveChatResponse, error) {
        return h.collector.StartCollectionArchiveLiveChat(request)
}

func (h *handler) GetArchiveLiveChat(ctx context.Context, request *GetArchiveLiveChatRequest) (*GetArchiveLiveChatResponse, error) {
        return h.collector.GetArchiveLiveChat(request)
}





func (c *collector) loopMain() {
        for {
                select {
		// チャンネル受け
		case <- reqchan
                case <-c.loopFinishResquestChan:
                        goto LAST
                }
        }
LAST:
        close(c.loopFinishResponseChan)
	}
}

func (c *collector) Start() {
	go loopMain
}

func (c *collector) Stop() {
	close(c.loopFinishResquestChan)
        <-c.;oopFinishResponseChan
}

func NewCollector(verbose bool, apiKeys []string, databasePath string) (*Searcher, error) {
	if len(apiKeys) != 1 {
		return fmt.Errorf("no api key")
	}
	databaseOperator := NewDatabaseOperator(verbose, databasePath)
	return &collector {
		 apiKey: apiKeys[0],
		 verbose bool,
	}
}
