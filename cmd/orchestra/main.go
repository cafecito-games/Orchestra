package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/cafecito-games/Orchestra/internal/cli"
)

func main() {
	operationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	exitCode := cli.Run(operationContext, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.LookupEnv)
	stopSignals()
	os.Exit(exitCode)
}
