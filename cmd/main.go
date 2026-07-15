/*
 * NWDAF - Network Data Analytics Function
 *
 * Nnwdaf_EventsSubscription Service
 * Based on 3GPP TS 29.520
 */

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/urfave/cli/v2"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/pkg/factory"
	"github.com/free5gc/nwdaf/pkg/service"
	logger_util "github.com/free5gc/util/logger"
)

var NWDAF *service.NwdafApp

func main() {
	os.Exit(runMain(os.Args))
}

func runMain(args []string) (exitCode int) {
	defer func() {
		if p := recover(); p != nil {
			logger.MainLog.Errorf("panic: %v\n%s", p, string(debug.Stack()))
			exitCode = 1
		}
	}()

	if err := run(args); err != nil {
		fmt.Fprintf(os.Stderr, "NWDAF Run Error: %v\n", err)
		return 1
	}
	return 0
}

func run(args []string) error {
	app := cli.NewApp()
	app.Name = "nwdaf"
	app.Usage = "5G Network Data Analytics Function (NWDAF)"
	app.Action = action
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Aliases: []string{"c"},
			Usage:   "Load configuration from `FILE`",
		},
		&cli.StringSliceFlag{
			Name:    "log",
			Aliases: []string{"l"},
			Usage:   "Output NF log to `FILE`",
		},
	}
	return app.Run(args)
}

func action(cliCtx *cli.Context) error {
	if err := initLogFile(cliCtx.StringSlice("log")); err != nil {
		return err
	}

	logger.MainLog.Infoln("NWDAF")
	logger.MainLog.Infoln("NWDAF version: v0.1.0")

	cfg, err := factory.ReadConfig(cliCtx.String("config"))
	if err != nil {
		return err
	}
	factory.NwdafConfig = cfg

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
	}()

	nwdaf, err := service.NewApp(ctx, cfg)
	if err != nil {
		return err
	}
	NWDAF = nwdaf

	return nwdaf.Run()
}

func initLogFile(logNfPath []string) error {
	for _, path := range logNfPath {
		if err := logger_util.LogFileHook(logger.Log, path); err != nil {
			return err
		}
	}
	return nil
}
