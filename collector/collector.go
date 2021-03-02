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

func NewCollector(verbose bool, apiKeys []string) (*Searcher, error) {
	if len(apiKeys) != 1 {
		return fmt.Errorf("no api key")
	}
	return &collector {
		 apiKey: apiKeys[0],
		 verbose bool,
	}
}
