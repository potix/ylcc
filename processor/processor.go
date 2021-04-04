package processor

type Processor struct {
	verbose   bool
	collector *collector.Collector
}

func (p *Processor)storeActiveLiveChat(videoId string) {
        subscribeActiveLiveChatParams := h.collector.subscribeActiveLiveChat(request)
        defer h.collector.unsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
        for {
                response, ok := <-subscribeActiveLiveChatParams.subscriberCh
                if !ok {
                        return nil
                }
                response
                if err := server.Send(response); err != nil {
                        return fmt.Errorf("can not send response: %w", err)
                }
        }
}

func (p *Processor) GetWordCloud(*pb.GetWordCloudRequest) (*GetWordCloudResponse, error) {

        response := h.collector.startCollectionActiveLiveChat(request)
        if response.Status.Code != pb.Code_SUCCESS && response.Status.Code != pb.Code_IN_PROGRESS {
                GetArchiveLiveChatResponse.Status = response.Status
        }

        subscribeActiveLiveChatParams := h.collector.subscribeActiveLiveChat(request)
        defer h.collector.unsubscribeActiveLiveChat(subscribeActiveLiveChatParams)
        for {
                response, ok := <-subscribeActiveLiveChatParams.subscriberCh
                if !ok {
                        return nil
                }
                if err := server.Send(response); err != nil {
                        return fmt.Errorf("can not send response: %w", err)
                }
        }


        return nil, status.Errorf(codes.Unimplemented, "method GetArchiveLiveChat not implemented")
        return nil, status.Errorf(codes.Unimplemented, "method GetWordCloud not implemented")
}

func NewProcessor(collector *collector.Collector) (*Processor) {
	return &Processor{
		collector: collector,
	}
}




