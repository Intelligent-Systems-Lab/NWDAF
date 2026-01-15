package processor

import (
	"context"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

type NwdafApp interface {
	CancelContext() context.Context
	Consumer() *consumer.Consumer
}

type Processor struct {
	nwdaf NwdafApp
}

func NewProcessor(nwdaf NwdafApp) *Processor {
	p := &Processor{
		nwdaf: nwdaf,
	}
	logger.ProcLog.Info("Processor initialized")
	return p
}
