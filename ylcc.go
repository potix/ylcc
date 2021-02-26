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

type ylccYoutubeConfig struct {
        APIKeyFile      string            `toml:"apiKeyFile"`
        PermitChannels  map[string]string `toml:"permitChannels"`
}

type ylccWebConfig struct {
        AddrPort string `toml:"addrPort"`
        Release  bool   `toml:"release"`
}

type ylccLogConfig struct {
        UseSyslog bool `toml:"useSyslog"`
}

type ylccConfig struct {
	Verbose  bool                  `toml:"verbose"`
        Youtube  *clipperYoutubeConfig `toml:"youtube"`
        Web      *clipperWebConfig     `toml:"web"`
        Cache    *clipperCacheConfig   `toml:"cache"`
        Log      *clipperLogConfig     `toml:"log"`
}

func verboseLoadedConfig(loadedConfig *ylccConfig) {
        if !ylccConfig.Verbose {
                return
        }
        j, err := json.Marshal(loadedConfig)
        if err != nil {
                log.Printf("can not dump config (%v)", err)
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
        conf := new(ylccConfig)
        err = cf.Load(conf)
        if err != nil {
                log.Printf("can not load config: %v", err)
                os.Exit(1)
        }
        if conf.Log.UseSyslog {
		logger, err := syslog.New(syslog.LOG_INFO|syslog.LOG_DAEMON, "ylcc")
                if err != nil {
                        log.Printf("can not create syslog: %v", err)
                        os.Exit(1)
                }
                log.SetOutput(logger)
        }
        verboseLoadedConfig(conf)
	newCollector := collector.NewCollector(
		conf.Youtube.APIKeyFile,
		conf.Youtube.Channels,
		conf.Verbose,
	)
	newHandler := handler.NewHandler(
		conf.Verbose,
		newCollector,
	)
        newServer := server.NewServer(
		conf.Web.AddrPort,
		"",
		"",
		conf.Web.Release,
		conf.Verbose,
		conf.Web.idleTimeout,
		conf.Web.shutdownTimeout,
		newHandler,
	)
	newCollector.Start()
	newServer.Start()
        signalWait()
        newServer.Stop()
	newCollector.Stop()
}
