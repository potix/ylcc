package main

import (
    "fmt"
    "flag"
    "context"
    "time"
    "io"
    "google.golang.org/grpc"
    pb "github.com/potix/ylcc/protocol"
)

const (
	addrPort = "127.0.0.1:12345"
)

func getVideo(client pb.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	request := &pb.GetVideoRequest {
		VideoId: videoId,
	}
	response, err := client.GetVideo(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v", response.Video)
	return
}

func startCollectionActiveLiveChat(client pb.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	request := &pb.StartCollectionActiveLiveChatRequest {
		VideoId: videoId,
	}
	response, err := client.StartCollectionActiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v", response.Video)
	return
}

func pollActiveLiveChat(client pb.YlccClient, videoId string) {
	ctx := context.Background()
	request := &pb.PollActiveLiveChatRequest {
		VideoId: videoId,
	}
	pollClient, err := client.PollActiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	for {
		response, err := pollClient.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Printf("%v", err)
			return
		}
		if response.Status.Code != pb.Code_SUCCESS {
			fmt.Printf("%v", response.Status.Message)
			return
		}
		for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
			fmt.Printf("%+v", activeLiveChatMessage)
		}
	}
}

func getCachedActiveLiveChat(client pb.YlccClient, videoId string, offset int64, count int64) (bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	request := &pb.GetCachedActiveLiveChatRequest {
		VideoId: videoId,
		Offset: offset,
		Count: count,
	}
	response, err := client.GetCachedActiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return false, err
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return false, fmt.Errorf("%v", response.Status.Message)
	}
	if len(response.ActiveLiveChatMessages) == 0 {
		return false, nil
	}
	for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
		fmt.Printf("%+v", activeLiveChatMessage)
	}
	return true, nil
}

func getCachedActiveLiveChatLoop(client pb.YlccClient, videoId string) {
	var offset int64 = 0
	var count int64 = 2000
	for {
		ok, err := getCachedActiveLiveChat(client, videoId, offset, count)
		if  err != nil {
			fmt.Printf("%v", err)
		}
		if !ok {
			break
		}
		offset += count
	}
}

func startCollectionArchiveLiveChat(client pb.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	request := &pb.StartCollectionArchiveLiveChatRequest {
		VideoId: videoId,
	}
	response, err := client.StartCollectionArchiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v", response.Video)
	return
}

func getArchiveLiveChat(client pb.YlccClient, videoId string, offset int64, count int64) (bool, error) {
RETRY:
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	request := &pb.GetArchiveLiveChatRequest {
		VideoId: videoId,
		Offset: offset,
		Count: count,
	}
	response, err := client.GetArchiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return false, err
	}
	if response.Status.Code == pb.Code_IN_PROGRESS {
		time.Sleep(5 * time.Second)
		goto RETRY
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return false, fmt.Errorf("%v", response.Status.Message)
	}
	if len(response.ArchiveLiveChatMessages) == 0 {
		return false, nil
	}
	for _, archiveLiveChatMessage := range response.ArchiveLiveChatMessages {
		fmt.Printf("%+v", archiveLiveChatMessage)
	}
	return true, nil
}

func getArchiveLiveChatLoop(client pb.YlccClient, videoId string) {
	var offset int64 = 0
	var count int64 = 2000
	for {
		ok, err := getArchiveLiveChat(client, videoId, offset, count)
		if  err != nil {
			fmt.Printf("%v", err)
		}
		if !ok {
			break
		}
		offset += count
	}
}

func main() {
	var mode  string
	var videoId string
	flag.StringVar(&mode, "mode", "active", "<active | activeCache | archive>")
	flag.StringVar(&videoId, "id", "", "<video id>")
        flag.Parse()
	conn, err := grpc.Dial(
		addrPort,
		grpc.WithInsecure(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Printf("can not create connection")
		return
	}
	defer conn.Close()
	client := pb.NewYlccClient(conn)
	switch (mode) {
	case "active":
		getVideo(client, videoId)
		startCollectionActiveLiveChat(client, videoId)
		pollActiveLiveChat(client, videoId)
	case "activeCache":
		getVideo(client, videoId)
		getCachedActiveLiveChatLoop(client, videoId)
	case "archive":
		getVideo(client, videoId)
		startCollectionArchiveLiveChat(client, videoId)
		getArchiveLiveChatLoop(client, videoId)
	}
}



