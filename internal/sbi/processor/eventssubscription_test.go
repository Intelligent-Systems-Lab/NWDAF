package processor

import (
	"context"
	"net/http"
	"slices"
	"testing"
	"time"

	"go.uber.org/mock/gomock"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
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
			name:    "Unsupported event ABNORMAL_BEHAVIOUR",
			event:   models.NwdafEvent_ABNORMAL_BEHAVIOUR,
			wantErr: true,
		},
		{
			name:    "Supported event UE_COMMUNICATION",
			event:   models.NwdafEvent_UE_COMMUNICATION,
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

type subscriptionTestApp struct {
	ctx      context.Context
	consumer *consumer.Consumer
}

func (a *subscriptionTestApp) SetLogEnable(bool) {}

func (a *subscriptionTestApp) SetLogLevel(string) {}

func (a *subscriptionTestApp) SetReportCaller(bool) {}

func (a *subscriptionTestApp) Start() {}

func (a *subscriptionTestApp) Terminate() {}

func (a *subscriptionTestApp) Config() *factory.Config {
	return nil
}

func (a *subscriptionTestApp) Context() *nwdaf_context.NWDAFContext {
	return nwdaf_context.GetSelf()
}

func (a *subscriptionTestApp) CancelContext() context.Context {
	return a.ctx
}

func (a *subscriptionTestApp) Consumer() *consumer.Consumer {
	return a.consumer
}

func TestValidateUeCommunication(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name     string
		eventSub *models.NwdafEventsSubscriptionEventSubscription
		wantErr  bool
		errCause string
	}{
		{
			name: "Valid with supis",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{
					Supis: []string{"imsi-208930000000003"},
				},
			},
			wantErr: false,
		},
		{
			name: "Valid with intGroupIds",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{
					IntGroupIds: []string{"group-123"},
				},
			},
			wantErr: false,
		},
		{
			name: "Missing tgtUe",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_UE_COMMUNICATION,
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
		{
			name: "Empty tgtUe (no supis or intGroupIds)",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{},
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
		{
			name: "tgtUe with only anyUe (not valid for UE_COMMUNICATION)",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{
					AnyUe: true,
				},
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateUeCommunication(tt.eventSub)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateUeCommunication() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateUeCommunication() cause = %v, want %v", err.Cause, tt.errCause)
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
			err := p.validateAbnormalBehaviourBasic(tt.eventSub)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateAbnormalBehaviourBasic() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateAbnormalBehaviourBasic() cause = %v, want %v", err.Cause, tt.errCause)
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
			err := p.validateSupportedExceptionIds(tt.excepRequs)
			if (err != nil) != tt.wantFail {
				t.Errorf("validateSupportedExceptionIds() fail = %v, wantFail %v", err != nil, tt.wantFail)
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
			name: "Supported event - no failures",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event: models.NwdafEvent_UE_COMMUNICATION,
				},
			},
			wantCount: 0,
		},
		{
			name: "Unsupported event - one failure",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event: models.NwdafEvent_UE_MOBILITY,
				},
			},
			wantCount: 1,
		},
		{
			name: "Mixed - one supported, one unsupported",
			eventSubs: []models.NwdafEventsSubscriptionEventSubscription{
				{
					Event: models.NwdafEvent_UE_COMMUNICATION,
				},
				{
					Event: models.NwdafEvent_UE_MOBILITY,
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

func TestValidateEventTargetPeriod(t *testing.T) {
	p := &Processor{}

	now := time.Now()
	pastTime := now.Add(-1 * time.Hour)
	futureTime := now.Add(1 * time.Hour)

	tests := []struct {
		name     string
		eventSub *models.NwdafEventsSubscriptionEventSubscription
		wantErr  bool
		errCause string
	}{
		{
			name: "startTs in past and endTs in future - BOTH_STAT_PRED_NOT_ALLOWED",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				ExtraReportReq: &models.EventReportingRequirement{
					StartTs: &pastTime,
					EndTs:   &futureTime,
				},
			},
			wantErr:  true,
			errCause: "BOTH_STAT_PRED_NOT_ALLOWED",
		},
		{
			name: "startTs after endTs - INVALID_REQUEST",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				ExtraReportReq: &models.EventReportingRequirement{
					StartTs: &futureTime,
					EndTs:   &pastTime,
				},
			},
			wantErr:  true,
			errCause: "INVALID_REQUEST",
		},
		{
			name: "Only startTs (past) - valid for statistics",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				ExtraReportReq: &models.EventReportingRequirement{
					StartTs: &pastTime,
				},
			},
			wantErr: false,
		},
		{
			name: "Only endTs (future) - valid for prediction",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
				ExtraReportReq: &models.EventReportingRequirement{
					EndTs: &futureTime,
				},
			},
			wantErr: false,
		},
		{
			name: "No extraReportReq - valid",
			eventSub: &models.NwdafEventsSubscriptionEventSubscription{
				Event: models.NwdafEvent_ABNORMAL_BEHAVIOUR,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.validateEventTargetPeriod(0, tt.eventSub)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateEventTargetPeriod() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errCause != "" && err.Cause != tt.errCause {
				t.Errorf("validateEventTargetPeriod() cause = %v, want %v", err.Cause, tt.errCause)
			}
		})
	}
}

func TestApplyAndValidateDefaults(t *testing.T) {
	p := &Processor{}

	tests := []struct {
		name       string
		req        *models.NnwdafEventsSubscription
		wantErr    bool
		wantStatus int
		errCause   string
	}{
		{
			name: "No evtReq - defaults to THRESHOLD - 501",
			req: &models.NnwdafEventsSubscription{
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
					{Event: models.NwdafEvent_UE_COMMUNICATION},
				},
			},
			wantErr:    true,
			wantStatus: 501,
			errCause:   "THRESHOLD_NOT_IMPLEMENTED",
		},
		{
			name: "Empty NotifMethod - defaults to THRESHOLD - 501",
			req: &models.NnwdafEventsSubscription{
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
					{Event: models.NwdafEvent_UE_COMMUNICATION},
				},
				EvtReq: &models.ReportingInformation{},
			},
			wantErr:    true,
			wantStatus: 501,
			errCause:   "THRESHOLD_NOT_IMPLEMENTED",
		},
		{
			name: "THRESHOLD explicitly specified - 501",
			req: &models.NnwdafEventsSubscription{
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
					{Event: models.NwdafEvent_UE_COMMUNICATION},
				},
				EvtReq: &models.ReportingInformation{
					NotifMethod: "THRESHOLD",
				},
			},
			wantErr:    true,
			wantStatus: 501,
			errCause:   "THRESHOLD_NOT_IMPLEMENTED",
		},
		{
			name: "PERIODIC without repPeriod - 400",
			req: &models.NnwdafEventsSubscription{
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
					{Event: models.NwdafEvent_UE_COMMUNICATION},
				},
				EvtReq: &models.ReportingInformation{
					NotifMethod: models.SmfEventExposureNotificationMethod_PERIODIC,
				},
			},
			wantErr:    true,
			wantStatus: 400,
			errCause:   "INVALID_REQUEST",
		},
		{
			name: "PERIODIC with repPeriod - valid",
			req: &models.NnwdafEventsSubscription{
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
					{Event: models.NwdafEvent_UE_COMMUNICATION},
				},
				EvtReq: &models.ReportingInformation{
					NotifMethod: models.SmfEventExposureNotificationMethod_PERIODIC,
					RepPeriod:   60,
				},
			},
			wantErr: false,
		},
		{
			name: "Unsupported notifMethod - 400",
			req: &models.NnwdafEventsSubscription{
				EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
					{Event: models.NwdafEvent_UE_COMMUNICATION},
				},
				EvtReq: &models.ReportingInformation{
					NotifMethod: "UNKNOWN_METHOD",
				},
			},
			wantErr:    true,
			wantStatus: 400,
			errCause:   "UNSUPPORTED_NOTIF_METHOD",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := p.applyAndValidateDefaults(tt.req)
			if (err != nil) != tt.wantErr {
				t.Errorf("applyAndValidateDefaults() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				if tt.wantStatus != 0 && int(err.Status) != tt.wantStatus {
					t.Errorf("applyAndValidateDefaults() status = %v, want %v", err.Status, tt.wantStatus)
				}
				if tt.errCause != "" && err.Cause != tt.errCause {
					t.Errorf("applyAndValidateDefaults() cause = %v, want %v", err.Cause, tt.errCause)
				}
			}
		})
	}
}

func TestHandleUpdateSubscription_ReappliesDefaultValidation(t *testing.T) {
	ctx := setupTestContext()
	p := newTestProcessor(t)

	subscriptionID := "sub-update-defaults"
	ctx.AddSubscription(&nwdaf_context.Subscription{
		ID:              subscriptionID,
		NotificationURI: "http://consumer.example/callback",
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{
			{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-old"}},
			},
		},
	})

	req := &models.NnwdafEventsSubscription{
		EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
			{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-new"}},
			},
		},
		NotificationURI: "http://consumer.example/callback",
	}

	response, problemDetails := p.HandleUpdateSubscription(subscriptionID, req)
	if response != nil {
		t.Fatalf("expected nil response on invalid update, got %+v", response)
	}
	if problemDetails == nil {
		t.Fatal("expected update to fail default validation")
	} else if int(problemDetails.Status) != http.StatusNotImplemented {
		t.Fatalf("status = %d, want %d", problemDetails.Status, http.StatusNotImplemented)
	} else if problemDetails.Cause != "THRESHOLD_NOT_IMPLEMENTED" {
		t.Fatalf("cause = %s, want THRESHOLD_NOT_IMPLEMENTED", problemDetails.Cause)
	}
}

func TestHandleUpdateSubscription_ReconcilesExternalState(t *testing.T) {
	ctx := setupTestContext()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	smfService := NewMockSmfServiceClient(ctrl)
	var subscribedSupis []string
	var unsubscribeCalls []string
	subscribeCount := 0
	smfService.EXPECT().
		SubscribeToSmf("http://smf.example", gomock.AssignableToTypeOf(consumer.SmfSubscriptionOptions{})).
		DoAndReturn(func(_ string, opts consumer.SmfSubscriptionOptions) (string, error) {
			subscribeCount++
			subscribedSupis = append(subscribedSupis, opts.Supi)
			return "smf-sub-" + opts.Supi, nil
		}).
		Times(2)
	smfService.EXPECT().
		UnsubscribeFromSmf("http://smf.example", gomock.Any()).
		DoAndReturn(func(_ string, subscriptionID string) error {
			unsubscribeCalls = append(unsubscribeCalls, subscriptionID)
			return nil
		}).
		Times(1)

	oldCfg := factory.NwdafConfig
	factory.NwdafConfig = &factory.Config{
		Configuration: &factory.Configuration{
			Smf: &factory.SmfConfig{
				Enabled:   true,
				Endpoints: []string{"http://smf.example"},
				NotifUris: &factory.NotifUris{
					Smf: "http://127.0.0.1:8080/collector/notify",
					Upf: "http://127.0.0.1:8080/collector/upf-notify",
				},
			},
		},
	}
	defer func() { factory.NwdafConfig = oldCfg }()

	consumerClient := consumer.NewConsumerWithServices(nil, smfService, nil, nil)

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p := NewProcessor(&subscriptionTestApp{
		ctx:      baseCtx,
		consumer: consumerClient,
	})

	subscriptionID := "sub-reconcile"
	ctx.AddSubscription(&nwdaf_context.Subscription{
		ID:              subscriptionID,
		NotificationURI: "http://consumer.example/callback",
		NotifCorrId:     "notif-old",
		EventSubs: []models.NwdafEventsSubscriptionEventSubscription{
			{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-old"}},
			},
		},
	})

	p.triggerTargetDataCollection(
		ctx,
		consumerClient,
		[]string{"http://smf.example"},
		[]DataCollectionTarget{{Supi: "imsi-old"}},
		subscriptionID,
		"http://127.0.0.1:8080/collector/notify",
		"http://127.0.0.1:8080/collector/upf-notify",
		10,
	)

	oldCorrelationID, found := ctx.GetSmfCorrelationId("supi=imsi-old", "http://smf.example")
	if !found {
		t.Fatal("expected initial SMF correlation for old target")
	}
	oldSmfSub := ctx.GetSmfSubscription(oldCorrelationID)
	if oldSmfSub == nil {
		t.Fatal("expected initial SMF subscription to exist")
	}
	_, oldSmfSubID, _ := oldSmfSub.GetInfo()

	mlInfo := nwdaf_context.NewMlModelInfo(models.NwdafEvent_UE_COMMUNICATION, "mtlf-old")
	mlInfo.SetModelUrl("file:///models/old-model")
	ctx.SetMlModelInfo(subscriptionID, mlInfo)
	shared, _ := ctx.GetOrCreateSharedModel("file:///models/old-model", models.NwdafEvent_UE_COMMUNICATION)
	shared.AddSubscriber(subscriptionID)

	past := time.Now().Add(-time.Second)
	req := &models.NnwdafEventsSubscription{
		EventSubscriptions: []models.NwdafEventsSubscriptionEventSubscription{
			{
				Event: models.NwdafEvent_UE_COMMUNICATION,
				TgtUe: &models.TargetUeInformation{Supis: []string{"imsi-new"}},
			},
		},
		NotificationURI: "http://consumer.example/new-callback",
		NotifCorrId:     "notif-new",
		EvtReq: &models.ReportingInformation{
			NotifMethod: models.SmfEventExposureNotificationMethod_PERIODIC,
			RepPeriod:   1,
			MonDur:      &past,
		},
	}

	response, problemDetails := p.HandleUpdateSubscription(subscriptionID, req)
	if problemDetails != nil {
		t.Fatalf("HandleUpdateSubscription returned problem: %+v", problemDetails)
	}
	if response == nil {
		t.Fatal("expected update response")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := ctx.GetSmfCorrelationId("supi=imsi-new", "http://smf.example"); ok {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, ok := ctx.GetSmfCorrelationId("supi=imsi-old", "http://smf.example"); ok {
		t.Fatal("old SMF correlation should be removed after update")
	}
	if ctx.GetSmfSubscription(oldCorrelationID) != nil {
		t.Fatal("old SMF subscription should be deleted after update")
	}

	newCorrelationID, ok := ctx.GetSmfCorrelationId("supi=imsi-new", "http://smf.example")
	if !ok {
		t.Fatal("new SMF correlation should exist after update")
	}
	if newCorrelationID == oldCorrelationID {
		t.Fatal("expected update to create a fresh correlation for the new target")
	}

	resources := ctx.GetNwdafSubResources(subscriptionID)
	if len(resources) != 1 {
		t.Fatalf("resource count = %d, want 1", len(resources))
	}
	if resources[0].Supi != "imsi-new" {
		t.Fatalf("resource supi = %s, want imsi-new", resources[0].Supi)
	}

	if ctx.GetMlModelInfo(subscriptionID) != nil {
		t.Fatal("stale ML model info should be cleared during update")
	}
	if ctx.GetSharedModel("file:///models/old-model") != nil {
		t.Fatal("stale shared model should be removed during update")
	}
	if subscribeCount != 2 {
		t.Fatalf("expected two SMF subscribe calls, got %d", subscribeCount)
	}
	if !slices.Equal(subscribedSupis, []string{"imsi-old", "imsi-new"}) {
		t.Fatalf("subscribed SUPIs = %v, want %v", subscribedSupis, []string{"imsi-old", "imsi-new"})
	}
	if len(unsubscribeCalls) != 1 {
		t.Fatalf("expected one SMF unsubscribe call, got %d", len(unsubscribeCalls))
	}
	if !slices.Contains(unsubscribeCalls, oldSmfSubID) {
		t.Fatalf("unsubscribe calls = %v, want %s", unsubscribeCalls, oldSmfSubID)
	}
}
