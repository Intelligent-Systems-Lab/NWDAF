package backend

import (
	"net/url"
	"testing"

	compatnrf "github.com/free5gc/nwdaf/internal/compat/nrf"
	"github.com/free5gc/openapi/models"
)

func TestParseNFDiscoveryQueryPreservesStandardShape(t *testing.T) {
	t.Parallel()

	values := url.Values{
		"target-nf-type":    {"NWDAF"},
		"requester-nf-type": {"NWDAF"},
		"service-names":     {"nnwdaf-mlmodelprovision"},
		"ml-analytics-info-list": {`[{
			"mlAnalyticsIds":["UE_COMMUNICATION"],
			"mlModelInterInfo":{"vendorList":["001122"]}
		}]`},
	}
	query, problem := ParseNFDiscoveryQuery(values)
	if problem != nil {
		t.Fatalf("ParseNFDiscoveryQuery() problem = %+v", problem)
	}
	if query.TargetNFType != models.NrfNfManagementNfType_NWDAF ||
		query.RequesterNFType != models.NrfNfManagementNfType_NWDAF ||
		len(query.MLAnalyticsInfoList) != 1 ||
		len(query.MLAnalyticsInfoList[0].MLAnalyticsIDs) != 1 ||
		query.MLAnalyticsInfoList[0].MLModelInteroperabilityInfo == nil {
		t.Fatalf("ParseNFDiscoveryQuery() query = %#v", query)
	}
}

func TestParseNFDiscoveryQueryFLClientAndADRF(t *testing.T) {
	t.Parallel()

	flQuery, problem := ParseNFDiscoveryQuery(url.Values{
		"target-nf-type": {"NWDAF"},
		"ml-analytics-info-list": {`[{
			"mlAnalyticsIds":["UE_COMMUNICATION"],
			"trackingAreaList":[{
				"plmnId":{"mcc":"466","mnc":"92"},
				"tac":"000001"
			}],
			"flCapabilityType":"FL_CLIENT",
			"nfTypeList":["UPF"]
		}]`},
	})
	if problem != nil || len(flQuery.MLAnalyticsInfoList) != 1 ||
		flQuery.MLAnalyticsInfoList[0].FLCapabilityType != compatnrf.FLCapabilityTypeClient {
		t.Fatalf("FL ParseNFDiscoveryQuery() query=%#v problem=%+v", flQuery, problem)
	}

	adrfQuery, problem := ParseNFDiscoveryQuery(url.Values{
		"target-nf-type":   {"ADRF"},
		"service-names":    {"nadrf-datamanagement"},
		"data-storage-ind": {"true"},
	})
	if problem != nil || adrfQuery.DataStorageInd == nil || !*adrfQuery.DataStorageInd {
		t.Fatalf("ADRF ParseNFDiscoveryQuery() query=%#v problem=%+v", adrfQuery, problem)
	}
}

func TestParseNFDiscoveryQueryRejectsForgedRequesterAndUnsupportedCombinations(t *testing.T) {
	t.Parallel()

	tests := []url.Values{
		{
			"target-nf-type":    {"SMF"},
			"requester-nf-type": {"AMF"},
			"service-names":     {"nsmf-event-exposure"},
		},
		{
			"target-nf-type": {"SMF"},
			"service-names":  {"nnwdaf-mlmodelprovision"},
		},
		{
			"target-nf-type":         {"NWDAF"},
			"ml-analytics-info-list": {"[]"},
		},
		{
			"target-nf-type":   {"NWDAF"},
			"data-storage-ind": {"true"},
		},
		{
			"target-nf-type": {"NWDAF"},
			"custom-filter":  {"value"},
		},
	}
	for _, values := range tests {
		if _, problem := ParseNFDiscoveryQuery(values); problem == nil ||
			problem.Status != 400 {
			t.Fatalf("ParseNFDiscoveryQuery(%v) problem = %+v", values, problem)
		}
	}
}
