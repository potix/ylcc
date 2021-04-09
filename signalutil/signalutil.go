package signalutil

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

func SignalWait(verbose bool) {
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
			if verbose {
				log.Printf("unexpected signal (sig = %v)", sig)
			}
		}
	}
}
