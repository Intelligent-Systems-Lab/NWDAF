package accuracy

import (
	"testing"
	"time"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
)

func makeDP(unixSec int64, ulVol, dlVol int64) nwdaf_context.UpfDataPoint {
	return nwdaf_context.UpfDataPoint{
		Timestamp: time.Unix(unixSec, 0),
		UlVolume:  ulVol,
		DlVolume:  dlVol,
	}
}

func setupCtx(t *testing.T) *nwdaf_context.NWDAFContext {
	t.Helper()
	nwdaf_context.Init()
	return nwdaf_context.GetSelf()
}

func snappedTs(unix int64) time.Time { return time.Unix(unix, 0).UTC() }

func setRawUpfData(
	ctx *nwdaf_context.NWDAFContext,
	corrID, ip string,
	points []nwdaf_context.UpfDataPoint,
) {
	data := ctx.GetOrCreateTrafficData(corrID, ip)
	data.Lock()
	data.RawUpfData = points
	data.Unlock()
}
