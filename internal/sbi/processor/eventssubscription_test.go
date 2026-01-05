package processor

import (
	"testing"

	"github.com/free5gc/openapi/models"
)

func TestValidateSupportedEvent(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name    string
		event   models.NwdafEvent
		wantErr bool
	}{
		{
			name:    "Supported event ABNORMAL_BEHAVIOUR",
			event:   models.NwdafEvent_ABNORMAL_BEHAVIOUR,
			wantErr: false,
		},
		{
			name:    "Unsupported event UE_MOBILITY",
			event:   models.NwdafEvent_UE_MOBILITY,
			wantErr: true,
		},
		{
			name:    "Unsupported event SLICE_LOAD_LEVEL",
			event:   models.NwdafEvent_SLICE_LOAD_LEVEL,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateSupportedEvent(tt.event)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSupportedEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateAbnormalBehaviour(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name     string
		eventSub *models.NwdafEventsSubscriptionEventSubscription
		wantErr  bool
		errCause string
	}{
		{
			name: "Valid with excepRequs",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				TgtUe: &models.TargetUeInformation{
					Supis: []string{"imsi-123456789"},
				},
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
				},
			},
			wantErr: false,
		},
		{
			name: "Valid with exptAnaType",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				TgtUe: &models.TargetUeInformation{
					Supis: []string{"imsi-123456789"},
				},
				ExptAnaType: models.ExpectedAnalyticsType_COMMUN,
			},
			wantErr: false,
		},
		{
			name: "Missing tgtUe",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
				},
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
		{
			name: "Mutual exclusion violation",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				TgtUe: &models.TargetUeInformation{
					Supis: []string{"imsi-123456789"},
				},
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
				},
				ExptAnaType: models.ExpectedAnalyticsType_COMMUN,
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
		{
			name: "Missing both excepRequs and exptAnaType",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				TgtUe: &models.TargetUeInformation{
					Supis: []string{"imsi-123456789"},
				},
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateAbnormalBehaviour(tt.eventSub)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateAbnormalBehaviour() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateAbnormalBehaviour() cause = %v, want %v", err.Cause, tt.errCause)
			}
		})
	}
}

func TestIsMobilityRelated(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name     string
		eventSub *models.NwdafEventsSubscriptionEventSubscription
		want     bool
	}{
		{
			name: "Mobility via exptAnaType",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				ExptAnaType: models.ExpectedAnalyticsType_MOBILITY,
			},
			want: true,
		},
		{
			name: "Mobility via excepRequs",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_UNEXPECTED_UE_LOCATION},
				},
			},
			want: true,
		},
		{
			name: "Commun only",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.isMobilityRelated(tt.eventSub)
			if got != tt.want {
				t.Errorf("isMobilityRelated() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestIsCommunRelated(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name     string
		eventSub *models.NwdafEventsSubscriptionEventSubscription
		want     bool
	}{
		{
			name: "Commun via exptAnaType",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				ExptAnaType: models.ExpectedAnalyticsType_COMMUN,
			},
			want: true,
		},
		{
			name: "Commun via DDOS excepRequs",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
				},
			},
			want: true,
		},
		{
			name: "Mobility only",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				ExcepRequs: []models.Exception{
					{ExcepId: models.ExceptionId_UNEXPECTED_UE_LOCATION},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.isCommunRelated(tt.eventSub)
			if got != tt.want {
				t.Errorf("isCommunRelated() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestValidateEvtReq(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name     string
		evtReq   *models.ReportingInformation
		wantErr  bool
		errCause string
	}{
		{
			name:    "Nil evtReq is valid",
			evtReq:  nil,
			wantErr: false,
		},
		{
			name: "Valid PERIODIC with repPeriod",
			evtReq: &models.ReportingInformation{
				NotifMethod: models.SmfEventExposureNotificationMethod_PERIODIC,
				RepPeriod:   60,
			},
			wantErr: false,
		},
		{
			name: "PERIODIC without repPeriod",
			evtReq: &models.ReportingInformation{
				NotifMethod: models.SmfEventExposureNotificationMethod_PERIODIC,
				RepPeriod:   0,
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
		{
			name: "Valid ONE_TIME",
			evtReq: &models.ReportingInformation{
				NotifMethod: models.SmfEventExposureNotificationMethod_ONE_TIME,
			},
			wantErr: false,
		},
		{
			name: "Negative maxReportNbr",
			evtReq: &models.ReportingInformation{
				MaxReportNbr: -1,
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateEvtReq(tt.evtReq)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateEvtReq() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateEvtReq() cause = %v, want %v", err.Cause, tt.errCause)
			}
		})
	}
}

func TestValidateSupportedExceptionIds(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name       string
		excepRequs []models.Exception
		wantErr    bool
		errCause   string
	}{
		{
			name: "Supported SUSPICION_OF_DDOS_ATTACK",
			excepRequs: []models.Exception{
				{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
			},
			wantErr: false,
		},
		{
			name: "Unsupported UNEXPECTED_UE_LOCATION",
			excepRequs: []models.Exception{
				{ExcepId: models.ExceptionId_UNEXPECTED_UE_LOCATION},
			},
			wantErr:  true,
			errCause: "UNSUPPORTED_EXCEPTION",
		},
		{
			name: "Unsupported WRONG_DESTINATION_ADDRESS",
			excepRequs: []models.Exception{
				{ExcepId: models.ExceptionId_WRONG_DESTINATION_ADDRESS},
			},
			wantErr:  true,
			errCause: "UNSUPPORTED_EXCEPTION",
		},
		{
			name:       "Empty list is valid",
			excepRequs: []models.Exception{},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateSupportedExceptionIds(tt.excepRequs)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSupportedExceptionIds() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateSupportedExceptionIds() cause = %v, want %v", err.Cause, tt.errCause)
			}
		})
	}
}

func TestValidateExptAnaType(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name        string
		exptAnaType models.ExpectedAnalyticsType
		wantErr     bool
		errCause    string
	}{
		{
			name:        "Supported COMMUN",
			exptAnaType: models.ExpectedAnalyticsType_COMMUN,
			wantErr:     false,
		},
		{
			name:        "Empty is valid",
			exptAnaType: "",
			wantErr:     false,
		},
		{
			name:        "Unsupported MOBILITY",
			exptAnaType: models.ExpectedAnalyticsType_MOBILITY,
			wantErr:     true,
			errCause:    "UNSUPPORTED_ANALYTICS_TYPE",
		},
		{
			name:        "Unsupported MOBILITY_AND_COMMUN",
			exptAnaType: models.ExpectedAnalyticsType_MOBILITY_AND_COMMUN,
			wantErr:     true,
			errCause:    "UNSUPPORTED_ANALYTICS_TYPE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateExptAnaType(tt.exptAnaType)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateExptAnaType() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateExptAnaType() cause = %v, want %v", err.Cause, tt.errCause)
			}
		})
	}
}

// Tests for Phase 2C: failEventReports functions

func TestCheckUnsupportedExceptionIds(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name       string
		excepRequs []models.Exception
		wantFail   bool
	}{
		{
			name: "Supported SUSPICION_OF_DDOS_ATTACK",
			excepRequs: []models.Exception{
				{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK},
			},
			wantFail: false,
		},
		{
			name: "Unsupported UNEXPECTED_UE_LOCATION",
			excepRequs: []models.Exception{
				{ExcepId: models.ExceptionId_UNEXPECTED_UE_LOCATION},
			},
			wantFail: true,
		},
		{
			name:       "Empty list",
			excepRequs: []models.Exception{},
			wantFail:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failInfo := p.checkUnsupportedExceptionIds(tt.excepRequs)
			if (failInfo != nil) != tt.wantFail {
				t.Errorf("checkUnsupportedExceptionIds() fail = %v, wantFail %v", failInfo != nil, tt.wantFail)
			}
			if failInfo != nil {
				if failInfo.Event != models.NwdafEvent_ABNORMAL_BEHAVIOUR {
					t.Errorf("checkUnsupportedExceptionIds() event = %v, want ABNORMAL_BEHAVIOUR", failInfo.Event)
				}
				if failInfo.FailureCode != models.NwdafFailureCode_OTHER {
					t.Errorf("checkUnsupportedExceptionIds() code = %v, want OTHER", failInfo.FailureCode)
				}
			}
		})
	}
}

func TestCheckUnsupportedExptAnaType(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name        string
		exptAnaType models.ExpectedAnalyticsType
		wantFail    bool
	}{
		{
			name:        "Supported COMMUN",
			exptAnaType: models.ExpectedAnalyticsType_COMMUN,
			wantFail:    false,
		},
		{
			name:        "Unsupported MOBILITY",
			exptAnaType: models.ExpectedAnalyticsType_MOBILITY,
			wantFail:    true,
		},
		{
			name:        "Empty is valid",
			exptAnaType: "",
			wantFail:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failInfo := p.checkUnsupportedExptAnaType(tt.exptAnaType)
			if (failInfo != nil) != tt.wantFail {
				t.Errorf("checkUnsupportedExptAnaType() fail = %v, wantFail %v", failInfo != nil, tt.wantFail)
			}
		})
	}
}

func TestCollectFailEventReports(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name      string
		eventSubs []models.NwdafEventsSubscriptionEventSubscription
		wantCount int
	}{
		{
			name: "No failures - supported ExceptionId",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event:      models.NwdafEvent_ABNORMAL_BEHAVIOUR,
					ExcepRequs: []models.Exception{{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK}},
				},
			},
			wantCount: 0,
		},
		{
			name: "One failure - unsupported ExceptionId",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event:      models.NwdafEvent_ABNORMAL_BEHAVIOUR,
					ExcepRequs: []models.Exception{{ExcepId: models.ExceptionId_UNEXPECTED_UE_LOCATION}},
				},
			},
			wantCount: 1,
		},
		{
			name: "One failure - unsupported exptAnaType",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event:       models.NwdafEvent_ABNORMAL_BEHAVIOUR,
					ExptAnaType: models.ExpectedAnalyticsType_MOBILITY,
				},
			},
			wantCount: 1,
		},
		{
			name: "Mixed - one success, one failure",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event:      models.NwdafEvent_ABNORMAL_BEHAVIOUR,
					ExcepRequs: []models.Exception{{ExcepId: models.ExceptionId_SUSPICION_OF_DDOS_ATTACK}},
				},
				{
					Event:       models.NwdafEvent_ABNORMAL_BEHAVIOUR,
					ExptAnaType: models.ExpectedAnalyticsType_MOBILITY,
				},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			failReports := p.collectFailEventReports(tt.eventSubs)
			if len(failReports) != tt.wantCount {
				t.Errorf("collectFailEventReports() count = %d, want %d", len(failReports), tt.wantCount)
			}
		})
	}
}
