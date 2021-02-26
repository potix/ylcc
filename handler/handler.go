package handler

import (
        "log"
        "github.com/gin-gonic/gin"
        "potix/ylc2wc/server"
        "potix/ylc2wc/collector"
)

type handler struct {
	collector *Collector
	verbose bool
}

// SetRouting is set routing
func (h *handler) SetRouting(router *gin.Engine) {
	router.Head("/ylcc/v1/Video/:videoId", h.videoCheck)
	router.Get("/ylcc/v1/Video/:videoId", h.videoGetInfo)
	router.Post("/ylcc/v1/Video/:videoId", h.videoCreate)
	router.Delete("/ylcc/v1/Video/:videoId", h.videoDelete)
	router.Head("/ylcc/v1/Video/:videoId/liveChat", h.videoWordCloudCheck)
	router.Get("/ylcc/v1/Video/:videoId/liveChat", h.videoWordCloudGetImage)
}

// Start is start handler
func (h *handler) Start() {
}

// Stop is stop handler
func (h *handler) Stop() {
}

// NewHandler is create new handler
func NewHandler(collector *collector.Collector, cacher *cacher.Cacher, verbose bool) (server.Handler) {
	return &handler{
		collector: collector,
		verbose:   verbose,
	}
}

func (h *handler) videoCheck(c *gin.Context) {
	// collectorのvideo情報存在チェック
	videoID := c.Param("videoId")
	if collector.Check(videoId) {
		c.JSON(200, gin.H{
			"status":  "posted",
			"message": message,
			"nick":    nick,
		})
	}

}

func (h *handler) videoGet(c *gin.Context) {
	// collectorのvideo情報取得
}

func (h *handler) videoCreate(c *gin.Context) {
	// collectorのvideo情報作成
}

func (h *handler) videoDelete(c *gin.Context) {
	// collectorのvideo情報削除
}

func (h *handler) videoLiveChatCheck(c *gin.Context) {
	// collectorのvideoのライブチャット情報存在チェック
}

func (h *handler) videoLiveChatGet(c *gin.Context) {
	// collectorのvideoのライブチャット情報取得
}

