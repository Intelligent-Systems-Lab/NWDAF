package processor

import (
	"context"

	"github.com/free5gc/nwdaf/internal/logger"
)

type NwdafApp interface {
	CancelContext() context.Context
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
