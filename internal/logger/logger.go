package logger

import (
	"os"

	"github.com/sirupsen/logrus"
)

var (
	Log          *logrus.Logger
	MainLog      *logrus.Entry
	InitLog      *logrus.Entry
	CfgLog       *logrus.Entry
	CtxLog       *logrus.Entry
	SBILog       *logrus.Entry
	ProcLog      *logrus.Entry
	ConsLog      *logrus.Entry
	GinLog       *logrus.Entry
	NotifierLog  *logrus.Entry
	CollectorLog *logrus.Entry
	AnlfLog      *logrus.Entry
	MtlfLog      *logrus.Entry
)

func init() {
	Log = logrus.New()
	Log.SetReportCaller(false)
	Log.SetFormatter(&logrus.TextFormatter{
		ForceColors:               true,
		DisableColors:             false,
		EnvironmentOverrideColors: false,
		DisableTimestamp:          false,
		FullTimestamp:             true,
		TimestampFormat:           "2006-01-02T15:04:05.000Z07:00",
	})
	Log.SetOutput(os.Stdout)
	Log.SetLevel(logrus.InfoLevel)

	MainLog = Log.WithField("component", "NWDAF").WithField("category", "Main")
	InitLog = Log.WithField("component", "NWDAF").WithField("category", "Init")
	CfgLog = Log.WithField("component", "NWDAF").WithField("category", "CFG")
	CtxLog = Log.WithField("component", "NWDAF").WithField("category", "CTX")
	SBILog = Log.WithField("component", "NWDAF").WithField("category", "SBI")
	ProcLog = Log.WithField("component", "NWDAF").WithField("category", "Proc")
	ConsLog = Log.WithField("component", "NWDAF").WithField("category", "Consumer")
	GinLog = Log.WithField("component", "NWDAF").WithField("category", "GIN")
	NotifierLog = Log.WithField("component", "NWDAF").WithField("category", "Notifier")
	CollectorLog = Log.WithField("component", "NWDAF").WithField("category", "Collector")
	AnlfLog = Log.WithField("component", "NWDAF").WithField("category", "AnLF")
	MtlfLog = Log.WithField("component", "NWDAF").WithField("category", "MTLF")
}
