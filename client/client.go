package main

import (
	"os"
	"context"
	"flag"
	"fmt"
	pb "github.com/potix/ylcc/protocol"
	"google.golang.org/grpc"
	"io"
	"time"
)

func getVideo(client pb.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	request := &pb.GetVideoRequest{
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
		60*time.Second,
	)
	defer cancel()
	request := &pb.StartCollectionActiveLiveChatRequest{
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
	request := &pb.PollActiveLiveChatRequest{
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
		60*time.Second,
	)
	defer cancel()
	request := &pb.GetCachedActiveLiveChatRequest{
		VideoId: videoId,
		Offset:  offset,
		Count:   count,
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
		if err != nil {
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
		60*time.Second,
	)
	defer cancel()
	request := &pb.StartCollectionArchiveLiveChatRequest{
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

func getArchiveLiveChat(client pb.YlccClient, videoId string, offset int64, count int64) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	request := &pb.GetArchiveLiveChatRequest{
		VideoId: videoId,
		Offset:  offset,
		Count:   count,
	}
	response, err := client.GetArchiveLiveChat(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return false, false, err
	}
	if response.Status.Code == pb.Code_IN_PROGRESS {
		return false, true, nil
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return false, false, fmt.Errorf("%v", response.Status.Message)
	}
	if len(response.ArchiveLiveChatMessages) == 0 {
		return false, false, nil
	}
	for _, archiveLiveChatMessage := range response.ArchiveLiveChatMessages {
		fmt.Printf("%+v", archiveLiveChatMessage)
	}
	return true, false, nil
}

func getArchiveLiveChatLoop(client pb.YlccClient, videoId string) {
	var offset int64 = 0
	var count int64 = 2000
	for {
		ok, retry, err := getArchiveLiveChat(client, videoId, offset, count)
		if err != nil {
			fmt.Printf("%v", err)
			return
		}
		if retry {
			time.Sleep(5 * time.Second)
			continue
		}
		if !ok {
			break
		}
		offset += count
	}
}

func startCollectionWordCloudMessages(client pb.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	request := &pb.StartCollectionWordCloudMessagesRequest{
		VideoId: videoId,
	}
	response, err := client.StartCollectionWordCloudMessages(ctx, request)
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

func getWordCloud(client pb.YlccClient, videoId string) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	colors := make([]*pb.Color, 0, 3)
	colors = append(colors, &pb.Color{
		R: 234,
		G: 112,
		B: 124,
		A: 255,
	})
	colors = append(colors, &pb.Color{
		R: 133,
		G: 233,
		B: 124,
		A: 255,
	})
	colors = append(colors, &pb.Color{
		R: 122,
		G: 125,
		B: 240,
		A: 255,
	})
	request := &pb.GetWordCloudRequest{
		VideoId: videoId,
		Target: pb.Target_ALL_USER,
		FontMaxSize: 64,
		FontMinSize: 16,
		Width: 1024,
		Height: 512,
		Colors: colors,
		BackgroundColor: &pb.Color{
			R: 255,
			G: 255,
			B: 255,
			A: 128,
		},
	}
	response, err := client.GetWordCloud(ctx, request)
	if err != nil {
		fmt.Printf("%v", err)
		return false, false, err
	}
	if response.Status.Code == pb.Code_IN_PROGRESS {
		return false, true, nil
	}
	if response.Status.Code != pb.Code_SUCCESS {
		return false, false, fmt.Errorf("%v", response.Status.Message)
	}
	file, err := os.Create("./output.png")
	if err != nil {
		return false, false, fmt.Errorf("can not create file: %v", err)
	}
	defer file.Close()
	_, err = file.Write(response.Data)
	if err != nil {
		return false, false, fmt.Errorf("can not write data to file: %v", err)
	}
	fmt.Printf("minetype = %v, length = %v\n", response.MimeType, len(response.Data))
	return true, false, nil
}

func getWordCloudLoop(client pb.YlccClient, videoId string) {
	for {
		ok, retry, err := getWordCloud(client, videoId)
		if err != nil {
			fmt.Printf("%v", err)
			return
		}
		if retry {
			time.Sleep(5 * time.Second)
			continue
		}
		if !ok {
			break
		}
		time.Sleep(5 * time.Second)
	}
}

func main() {
	var mode string
	var videoId string
	var addrPort string
	flag.StringVar(&mode, "mode", "active", "<active | activeCache | archive | wordCloud>")
	flag.StringVar(&videoId, "id", "", "<video id>")
	flag.StringVar(&addrPort, "to", "127.0.0.1:12345", "<video id>")
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
	switch mode {
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
	case "wordCloud":
		getVideo(client, videoId)
		startCollectionWordCloudMessages(client, videoId)
		getWordCloudLoop(client, videoId)
	}
}
