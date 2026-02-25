package processor

import (
	"context"
	"sync"

	"github.com/free5gc/nwdaf/internal/logger"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

type NwdafApp interface {
	CancelContext() context.Context
	Consumer() *consumer.Consumer
}

type Processor struct {
	nwdaf NwdafApp
	wg    *sync.WaitGroup
}

func NewProcessor(nwdaf NwdafApp) *Processor {
	p := &Processor{
		nwdaf: nwdaf,
	}
	logger.ProcLog.Info("Processor initialized")
	return p
}

// SetWaitGroup stores the application WaitGroup for goroutine lifecycle management
func (p *Processor) SetWaitGroup(wg *sync.WaitGroup) {
	p.wg = wg
}
