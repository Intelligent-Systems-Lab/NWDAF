package mtlf

import (
	"context"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/pkg/factory"
)

type testNwdafApp struct {
	ctx context.Context
	cfg *factory.Config
}

func (a testNwdafApp) SetLogEnable(bool) {}

func (a testNwdafApp) SetLogLevel(string) {}

func (a testNwdafApp) SetReportCaller(bool) {}

func (a testNwdafApp) Start() {}

func (a testNwdafApp) Terminate() {}

func (a testNwdafApp) Config() *factory.Config {
	return a.cfg
}

func (a testNwdafApp) Context() *nwdaf_context.NWDAFContext {
	return nwdaf_context.GetSelf()
}

func (a testNwdafApp) CancelContext() context.Context {
	if a.ctx != nil {
		return a.ctx
	}
	return context.Background()
}

func newTestMtlfService(cfg *factory.Config) *MtlfService {
	return NewMtlfService(testNwdafApp{
		ctx: context.Background(),
		cfg: cfg,
	}, nil, nil)
}
