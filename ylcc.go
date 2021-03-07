package main


import (
        "os"
        "log"
        "log/syslog"
        "flag"
        "syscall"
        "os/signal"
        "encoding/json"
        "github.com/potix/ylcc/configurator"
        "github.com/potix/ylcc/collector"
        "github.com/potix/ylcc/handler"
        "github.com/potix/ylcc/server"
)

type ylccCollectorConfig struct {
        ApiKeyFile  string `toml:"apiKeyFile"`
	DatabsePath string `toml:"databasePath"`
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
	Verbose   bool                    `toml:"verbose"`
        Collector *clipperCollectorConfig `toml:"collector"`
        Server    *clipperServerConfig    `toml:"server"`
        Log       *clipperLogConfig       `toml:"log"`
}

func verboseLoadedConfig(loadedConfig *ylccConfig) {
        if !ylccConfig.Verbose {
                return
        }
        j, err := json.Marshal(loadedConfig)
        if err != nil {
		log.Printf("can not dump config: %v", err)
                return
        }
        log.Printf("loaded config: %v", string(j))
}

func signalWait() {
        sigChan := make(chan os.Signal, 1)
        signal.Notify(sigChan,
                syscall.SIGINT,
                syscall.SIGQUIT,
                syscall.SIGTERM)
        for {
                sig := <-sigChan
                switch sig {
                case syscall.SIGINT:
                        fallthrough
                case syscall.SIGQUIT:
                        fallthrough
                case syscall.SIGTERM:
                        return
                default:
                        log.Printf("unexpected signal (sig = %v)", sig)
                }
        }
}

func main() {
        cmdArgs := new(commandArguments)
        flag.StringVar(&cmdArgs.configFile, "config", "/usr/local/etc/ylcc.conf", "config file")
        flag.BoolVar(&cmdArgs.verbose, "verbose", false, "verbose")
        flag.Parse()
        cf, err := configurator.NewConfigurator(cmdArgs.configFile)
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
        verboseLoadedConfig(conf)
	apiKeys, err := configurator.LoadSecetFile(conf.collector.ApiKeyFile)
	if err != nil {
                log.Fatalf("can not load secret file: %v", conf.collector.ApiKeyFile)
	}
	newCollector := collector.NewCollector(
		conf.Verbose,
		apiKeys,
		conf.Collector.DatabasePath,
	)
	newHandler := handler.NewHandler(
		conf.Verbose,
		newCollector,
	)
        newServer := server.NewServer(
		conf.Verbose,
		conf.Server.AddrPort,
		conf.Server.TlsCertPath,
		conf.Server.TlsKeyPath,
		newHandler,
	)
	newServer.Start()
        signalWait()
        newServer.Stop()
}
