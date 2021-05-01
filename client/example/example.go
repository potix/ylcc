package main

import (
	"os"
	"context"
	"flag"
	"fmt"
	"time"
	"google.golang.org/grpc"
	pb "github.com/potix/ylcc/protocol"
	"github.com/potix/ylcc/client"
)

func getVideo(client *client.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.GetVideo(ctx, videoId)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v\n", response.Video)
	return
}

func startCollectionActiveLiveChat(client *client.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.StartCollectionActiveLiveChat(ctx, videoId)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v\n", response.Video)
	return
}

func pollActiveLiveChat(client *client.YlccClient, videoId string) {
	ctx := context.Background()
	err := client.PollActiveLiveChat(ctx, videoId, func(response *pb.PollActiveLiveChatResponse)(bool) {
		if response.Status.Code != pb.Code_SUCCESS {
			fmt.Printf("%v", response.Status.Message)
			return true
		}
		for _, activeLiveChatMessage := range response.ActiveLiveChatMessages {
			fmt.Printf("%+v", activeLiveChatMessage)
		}
		return false
	})
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
}

func getCachedActiveLiveChat(client *client.YlccClient, videoId string, offset int64, count int64) (bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.GetCachedActiveLiveChat(ctx, videoId, offset, count)
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
		fmt.Printf("%+v\n", activeLiveChatMessage)
	}
	return true, nil
}

func getCachedActiveLiveChatLoop(client *client.YlccClient, videoId string) {
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

func startCollectionArchiveLiveChat(client *client.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.StartCollectionArchiveLiveChat(ctx, videoId)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v\n", response.Video)
	return
}

func getArchiveLiveChat(client *client.YlccClient, videoId string, offset int64, count int64) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.GetArchiveLiveChat(ctx, videoId, offset, count)
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
		fmt.Printf("%+v\n", archiveLiveChatMessage)
	}
	return true, false, nil
}

func getArchiveLiveChatLoop(client *client.YlccClient, videoId string) {
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

func startCollectionWordCloudMessages(client *client.YlccClient, videoId string) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.StartCollectionWordCloudMessages(ctx, videoId)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	if response.Status.Code != pb.Code_SUCCESS {
		fmt.Printf("%v", response.Status.Message)
		return
	}
	fmt.Printf("%+v\n", response.Video)
	return
}

func getWordCloud(client *client.YlccClient, videoId string) (bool, bool, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	colors := make([]*pb.Color, 0, 3)
	colors = append(colors, client.BuildRGBColor(234, 112, 124))
	colors = append(colors, client.BuildRGBColor(133, 233, 124))
	colors = append(colors, client.BuildRGBColor(122, 125, 240))
	bgColor := client.BuildRGBColor(255, 255, 255)
	response, err := client.GetWordCloud(ctx, videoId, pb.Target_ALL_USER, 10, 64, 16, 1024, 512, colors, bgColor)
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

func getWordCloudLoop(client *client.YlccClient, videoId string) {
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

func openVote(client *client.YlccClient, videoId string) (string, error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	choices := make([]*pb.VoteChoice, 0, 4)
	choices = append(choices, client.BuildChoice("あ", "ああああああ"))
	choices = append(choices, client.BuildChoice("い", "いいいいいい"))
	choices = append(choices, client.BuildChoice("う", "ううううう"))
	choices = append(choices, client.BuildChoice("え", "ええええええ"))
	response, err := client.OpenVote(ctx, videoId, pb.Target_ALL_USER, 120, choices)
	if err != nil {
		fmt.Printf("%v", err)
		return "", err
	}
	if response.Status.Code != pb.Code_SUCCESS {
		return "", fmt.Errorf("%v", response.Status.Message)
	}
	fmt.Printf("%+v\n", response.Video)
	return response.VoteId, nil
}

func getVoteResult(client *client.YlccClient, voteId string) (error){
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	response, err := client.GetVoteResult(ctx, voteId)
	if err != nil {
		fmt.Printf("%v", err)
		return err
	}
	if response.Status.Code != pb.Code_SUCCESS {
		return fmt.Errorf("%v", response.Status.Message)
	}
	fmt.Printf("total: %v\n", response.Total)
	fmt.Printf("Counts: %+v\n", response.Counts)
	return nil
}

func updateVoteDuration(client *client.YlccClient, voteId string) (error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60*time.Second,
	)
	defer cancel()
	response, err := client.UpdateVoteDuration(ctx, voteId, 120)
	if err != nil {
		fmt.Printf("%v", err)
		return err
	}
	if response.Status.Code != pb.Code_SUCCESS {
		return fmt.Errorf("%v", response.Status.Message)
	}
	return nil
}

func closeVote(client *client.YlccClient, voteId string) (error) {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		60 * time.Second,
	)
	defer cancel()
	response, err := client.CloseVote(ctx, voteId)
	if err != nil {
		fmt.Printf("%v", err)
		return err
	}
	if response.Status.Code != pb.Code_SUCCESS {
		return fmt.Errorf("%v", response.Status.Message)
	}
	return nil
}

func voteLoop(client *client.YlccClient, videoId string) {
	voteId, err := openVote(client, videoId)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
	for i := 0; i < 10; i += 1{
		time.Sleep(60 * time.Second)
		err = getVoteResult(client, voteId)
		if err != nil {
			fmt.Printf("%v", err)
			return
		}
		err = updateVoteDuration(client, voteId)
		if err != nil {
			fmt.Printf("%v", err)
			return
		}
	}
	time.Sleep(300 * time.Second)
	err = closeVote(client, voteId)
	if err != nil {
		fmt.Printf("%v", err)
		return
	}
}

func main() {
	var mode string
	var videoId string
	var addrPort string
	flag.StringVar(&mode, "mode", "active", "<active | activeCache | archive | wordCloud | vote>")
	flag.StringVar(&videoId, "id", "", "<video id>")
	flag.StringVar(&addrPort, "to", "127.0.0.1:12345", "<video id>")
	flag.Parse()
	if videoId == "" {
		flag.Usage()
		return
	}
	client := client.NewYlccClient(
		addrPort,
		grpc.WithInsecure(),
		grpc.FailOnNonTempDialError(true),
		grpc.WithBlock(),
	)
	err := client.Dial()
	if err != nil {
		fmt.Printf("can not create connection")
		return
	}
	defer client.Close()
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
	case "vote":
		getVideo(client, videoId)
		voteLoop(client, videoId)
	}
}
