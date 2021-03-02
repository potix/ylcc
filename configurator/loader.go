package configurator

import (
	"fmt"
	"github.com/BurntSushi/toml"
)

type loader struct {
	configFile string
}

func (l *loader) loadToml(config interface{}) (error) {
	_, err := toml.DecodeFile(l.configFile, config)
	if err != nil {
		return fmt.Errorf("can not decode config file for toml (%v): %w", l.configFile, err)
	}
	return nil
}

func (l *loader)load(config interface{}) (error) {
	err := l.loadToml(config)
	if err != nil {
		return fmt.Errorf(err, "can not load config file (%v): %w", l.configFile, err)
	}
	return nil
}

func newLoader(configFile string) (*loader) {
	return &loader{
            configFile: configFile,
        }
}
