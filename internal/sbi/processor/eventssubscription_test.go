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
