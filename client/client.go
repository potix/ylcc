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


func getCachedActiveLiveChat(client pb.YlccClient, videoId string, pageToken string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	request := &pb.GetCachedActiveLiveChatRequest {
		VideoId: videoId,
		PageToken: pageToken,
	}
	response, err := client.GetCachedActiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return "", err
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return "", fmt.Errorf("%v", response.Status.Message)
	}
	if len(response.ActiveLiveChatMessages) == 0 {
		fmt.Printf("no message")
		return "", fmt.Errorf("no message")
	}
	for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
		fmt.Printf("%+v", activeLiveChatMessage)
	}
	return response.NextPageToken, nil
}


func getCachedActiveLiveChatLoop(client pb.YlccClient, videoId string) {
	pageToken := ""
	for {
		nextPageToken, err := getCachedActiveLiveChat(client, videoId, pageToken)
		if  err != nil {
			fmt.Printf("%v", err)
		}
		if nextPageToken == "" {
			break
		}
		pageToken = nextPageToken
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
		startCollectionActiveLiveChat(client, videoId)
		pollActiveLiveChat(client, videoId)
		getVideo(client, videoId)
	case "activeCache":
		getCachedActiveLiveChatLoop(client, videoId)
		getVideo(client, videoId)
	case "archive":
		//startCollectionArchiveLiveChat(client, videoId)
		//getArchiveLiveChat(client, videoId)
		getVideo(client, videoId)
	}
}



