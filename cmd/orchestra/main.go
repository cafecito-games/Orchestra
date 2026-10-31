package main

import (
	"context"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/cafecito-games/Orchestra/internal/cli"
	"github.com/cafecito-games/Orchestra/internal/harness"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	return runWithIO(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, os.LookupEnv)
}

func runWithIO(
	arguments []string,
	standardInput io.Reader,
	standardOutput io.Writer,
	standardError io.Writer,
	lookupEnvironment func(string) (string, bool),
) int {
	harness.RegisterOrchestraExecutableFromOS()
	operationContext, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	return cli.Run(operationContext, arguments, standardInput, standardOutput, standardError, lookupEnvironment)
}
