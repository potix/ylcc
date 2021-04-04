package main


import (
        "log"
        "log/syslog"
        "flag"
        "encoding/json"
        "github.com/potix/ylcc/signalutil"
        "github.com/potix/ylcc/configurator"
        "github.com/potix/ylcc/collector"
        "github.com/potix/ylcc/processor"
        "github.com/potix/ylcc/handler"
        "github.com/potix/ylcc/server"
)

type ylccCollectorConfig struct {
        ApiKeyFile   string `toml:"apiKeyFile"`
	DatabasePath string `toml:"databasePath"`
}

type ylccServerConfig struct {
        AddrPort    string `toml:"addrPort"`
        TlsCertPath string `toml:"tlsCertPath"`
        TlsKeyPath  string `toml:"tlsKeyPath"`
}

type ylccLogConfig struct {
        UseSyslog bool `toml:"useSyslog"`
}

type ylccConfig struct {
	Verbose   bool                 `toml:"verbose"`
        Collector *ylccCollectorConfig `toml:"collector"`
        Server    *ylccServerConfig    `toml:"server"`
        Log       *ylccLogConfig       `toml:"log"`
}

type commandArguments struct {
	configFile string
}

func verboseLoadedConfig(config *ylccConfig) {
        if !config.Verbose {
                return
        }
        j, err := json.Marshal(config)
        if err != nil {
		log.Printf("can not dump config: %v", err)
                return
        }
        log.Printf("loaded config: %v", string(j))
}

func main() {
        cmdArgs := new(commandArguments)
        flag.StringVar(&cmdArgs.configFile, "config", "./ylcc.conf", "config file")
        flag.Parse()
        cf, err := configurator.NewConfigurator(cmdArgs.configFile)
        if err != nil {
                log.Fatalf("can not create configurator: %v", err)
        }
        var conf ylccConfig
        err = cf.Load(&conf)
        if err != nil {
                log.Fatalf("can not load config: %v", err)
        }
        if conf.Log.UseSyslog {
		logger, err := syslog.New(syslog.LOG_INFO|syslog.LOG_DAEMON, "ylcc")
                if err != nil {
                        log.Fatalf("can not create syslog: %v", err)
                }
                log.SetOutput(logger)
        }
        verboseLoadedConfig(&conf)
	apiKeys, err := configurator.LoadSecretFile(conf.Collector.ApiKeyFile)
	if err != nil {
		log.Fatalf("can not load secret file %v: %v", conf.Collector.ApiKeyFile, err)
	}
	if len(apiKeys) != 1 {
                log.Fatalf("no api key")
        }
	newCollector, err := collector.NewCollector(
		conf.Verbose,
		apiKeys,
		conf.Collector.DatabasePath,
	)
	if err != nil {
		log.Fatalf("can not create controller: %v", err)
	}
	newProcessor := processor.NewProcessor(
		conf.Verbose,
		newCollector,
	)
	newHandler := collector.NewHandler(
		conf.Verbose,
		newProcessor,
		newCollector,
	)
        newServer, err := server.NewServer(
		conf.Verbose,
		conf.Server.AddrPort,
		conf.Server.TlsCertPath,
		conf.Server.TlsKeyPath,
		newHandler,
	)
	if err != nil {
		log.Fatalf("can not create server: %v", err)
	}
	err = newServer.Start()
	if err != nil {
                log.Fatalf("can not start server: %v", err)
	}
        signalutil.SignalWait(conf.Verbose)
        newServer.Stop()
}
