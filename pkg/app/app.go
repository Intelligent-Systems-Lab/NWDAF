package app

import (
	"github.com/free5gc/nwdaf/pkg/factory"
)

type App interface {
	Config() *factory.Config
	SetLogEnable(enable bool)
	SetLogLevel(level string)
	SetReportCaller(reportCaller bool)
	Terminate()
}
