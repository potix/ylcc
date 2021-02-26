package collector

import (
        "os"
        "strings"
        "io/ioutil"
        "github.com/pkg/errors"
)


type collector struct {
	verbose bool
	apiKey  string
}

func (c *collector) loopMain() {
        for {
                select {
                case <-time.After(time.Second):
                        c.invokeMonitorGoroutine()
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
