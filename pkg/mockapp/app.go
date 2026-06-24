package mockapp

import (
	"context"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/app"
)

//go:generate /home/x81u/go/bin/mockgen -source=app.go -destination=mock.go -package=mockapp

// App is the richer shared test seam used by packages that need more than the
// narrow root app.App contract.
type App interface {
	app.App
	CancelContext() context.Context
	Consumer() consumer.ConsumerAPI
}
