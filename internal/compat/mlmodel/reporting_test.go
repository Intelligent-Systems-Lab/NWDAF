package mlmodel

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestReportingInformationPreservesOptionalScalarPresence(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"immRep":false,
		"maxReportNbr":0,
		"repPeriod":0,
		"sampRatio":0,
		"grpRepTime":0
	}`)
	var value ReportingInformation
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if value.ImmediateReport == nil || *value.ImmediateReport {
		t.Fatalf("immRep = %v, want explicit false", value.ImmediateReport)
	}
	for name, field := range map[string]*int64{
		"maxReportNbr": value.MaximumReportCount,
		"repPeriod":    value.RepetitionPeriod,
		"sampRatio":    value.SamplingRatio,
		"grpRepTime":   value.GroupReportingTime,
	} {
		if field == nil || *field != 0 {
			t.Fatalf("%s = %v, want explicit zero", name, field)
		}
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, field := range []string{"immRep", "maxReportNbr", "repPeriod", "sampRatio", "grpRepTime"} {
		if !strings.Contains(string(encoded), `"`+field+`"`) {
			t.Fatalf("json.Marshal() dropped %s: %s", field, encoded)
		}
	}
}

func TestReportingInformationAcceptsCompleteRelease18Shape(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"immRep":true,
		"notifMethod":"PERIODIC",
		"maxReportNbr":4,
		"monDur":"2026-07-28T12:00:00Z",
		"repPeriod":30,
		"sampRatio":50,
		"partitionCriteria":["TAC"],
		"grpRepTime":10,
		"notifFlag":"ACTIVATE",
		"notifFlagInstruct":{
			"bufferedNotifs":"DROP_OLD",
			"subscription":"CONTINUE_WITHOUT_MUTING"
		},
		"mutingSetting":{
			"maxNoOfNotif":100,
			"durationBufferedNotif":60
		}
	}`)
	var value ReportingInformation
	if err := json.Unmarshal(body, &value); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if err := ValidateReportingInformation(&value); err != nil {
		t.Fatalf("ValidateReportingInformation() error = %v", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for _, field := range []string{"notifFlagInstruct", "mutingSetting", "partitionCriteria"} {
		if !strings.Contains(string(encoded), `"`+field+`"`) {
			t.Fatalf("json.Marshal() dropped %s: %s", field, encoded)
		}
	}
}

func TestReportingInformationRejectsInvalidCardinalityAndRanges(t *testing.T) {
	t.Parallel()

	for _, body := range []string{
		`{"sampRatio":101}`,
		`{"repPeriod":-1}`,
		`{"partitionCriteria":[]}`,
		`{"mutingSetting":{"maxNoOfNotif":-1}}`,
	} {
		var value ReportingInformation
		if err := json.Unmarshal([]byte(body), &value); err != nil {
			t.Fatalf("json.Unmarshal(%s) error = %v", body, err)
		}
		if err := ValidateReportingInformation(&value); err == nil {
			t.Fatalf("ValidateReportingInformation(%s) unexpectedly succeeded", body)
		}
	}
}
