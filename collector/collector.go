package collector

import (
        "os"
        "strings"
        "io/ioutil"
        "github.com/pkg/errors"
)

func loadApiKey(apiKeyFile string) ([]string, error) {
        fileInfo, err := os.Stat(apiKeyFile)
        if err != nil {
                return nil, errors.Wrapf(err, "not exists youtube data api key file (%v)", apiKeyFile)
        }
        if fileInfo.Mode().Perm() != 0600 {
                return nil, errors.Errorf("youtube data api key file have insecure permission (e.g. !=  0600) (%v)", apiKeyFile)
        }
        apiKeysBytes, err := ioutil.ReadFile(apiKeyFile)
        if err != nil {
                return nil, errors.Wrapf(err, "can not read youtube data api key file (%v)", apiKeyFile)
        }
        apiKeysStrings := strings.Split(string(apiKeysBytes), "\n")
        apiKeys := make([]string, 0, len(apiKeysStrings))
        for _, s := range apiKeysStrings {
                apiKey := strings.TrimSpace(s)
                if strings.HasPrefix(apiKey, "#") {
                        continue
                }
                if apiKey == "" {
                        continue
                }
                apiKeys = append(apiKeys, apiKey)
        }
        return apiKeys, nil
}



func NewCollector(apiKeys []string, channels []*Channel, verbose bool, cacher *cacher.Cacher) (*Searcher, error) {
        ctxs := make([]context.Context, 0, len(apiKeys))
        youtubeServices := make([]*youtube.Service, 0, len(apiKeys))
        for _, apiKey := range apiKeys {
                ctx := context.Background()
                youtubeService, err := youtube.NewService(ctx, option.WithAPIKey(apiKey))
                if err != nil {
                        return nil, errors.Wrapf(err, "can not create youtube service")
                }
                ctxs = append(ctxs, ctx)
                youtubeServices = append(youtubeServices, youtubeService)
        }
        return &Searcher{
                apiKeys: apiKeys,
                maxVideos: maxVideos,
                scraping: scraping,
                channels: channels,
                ctxs: ctxs,
                youtubeServices: youtubeServices,
                youtubeServicesIdx: 0,
                databaseOperator: databaseOperator,
                verbose: verbose,
        }, nil
}
