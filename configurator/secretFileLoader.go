package configurator

import (
        "os"
        "strings"
        "io/ioutil"
)

func LoadSecretFile(secretFile string) ([]string, error) {
        fileInfo, err := os.Stat(secretFile)
        if err != nil {
		return nil, fmt.Errorf("not exists secret file (%v): %w", secretFile, err)
        }
        if fileInfo.Mode().Perm() != 0600 {
                return nil, fmt.Errorf("secret file have insecure permission (e.g. !=  0600) (%v)", secretFile)
        }
        loadedBytes, err := ioutil.ReadFile(secretFile)
        if err != nil {
		return nil, fmt.Errorf("can not read youtube data api key file (%v): %w", err)
        }
        loadedStrings := strings.Split(string(loadedBytes), "\n")
        strings := make([]string, 0, len(loadedStrings))
        for _, s := range loadedStrings {
                ts := strings.TrimSpace(s)
                if strings.HasPrefix(ts, "#") {
                        continue
                }
                if ts == "" {
                        continue
                }
                strings = append(strings, ts)
        }
        return strings, nil
}
