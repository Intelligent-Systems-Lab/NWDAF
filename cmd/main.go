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
	defer func() {
		if p := recover(); p != nil {
			logger.MainLog.Fatalf("panic: %v\n%s", p, string(debug.Stack()))
		}
	}()

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
	if err := app.Run(os.Args); err != nil {
		fmt.Printf("NWDAF Run Error: %v\n", err)
	}
}

func action(cliCtx *cli.Context) error {
	if err := initLogFile(cliCtx.StringSlice("log")); err != nil {
		return err
	}

	logger.MainLog.Infoln("NWDAF")
	logger.MainLog.Infoln("NWDAF version: v0.1.0")

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
	}()

	cfg, err := factory.ReadConfig(cliCtx.String("config"))
	if err != nil {
		return err
	}
	factory.NwdafConfig = cfg

	nwdaf, err := service.NewApp(ctx, cfg)
	if err != nil {
		return err
	}
	NWDAF = nwdaf

	nwdaf.Start()

	return nil
}

func initLogFile(logNfPath []string) error {
	for _, path := range logNfPath {
		if err := logger_util.LogFileHook(logger.Log, path); err != nil {
			return err
		}
	}
	return nil
}
