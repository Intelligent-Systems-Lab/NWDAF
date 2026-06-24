package processor

import (
	"context"
	"net/http"
	"reflect"

	gomock "go.uber.org/mock/gomock"

	nwdaf_context "github.com/free5gc/nwdaf/internal/context"
	"github.com/free5gc/nwdaf/internal/sbi/consumer"
	"github.com/free5gc/nwdaf/pkg/factory"
)

// MockNwdafApp is a mock of NwdafApp interface.
type MockNwdafApp struct {
	ctrl     *gomock.Controller
	recorder *MockNwdafAppMockRecorder
}

// MockNwdafAppMockRecorder is the mock recorder for MockNwdafApp.
type MockNwdafAppMockRecorder struct {
	mock *MockNwdafApp
}

// NewMockNwdafApp creates a new mock instance.
func NewMockNwdafApp(ctrl *gomock.Controller) *MockNwdafApp {
	mock := &MockNwdafApp{ctrl: ctrl}
	mock.recorder = &MockNwdafAppMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockNwdafApp) EXPECT() *MockNwdafAppMockRecorder {
	return m.recorder
}

// CancelContext mocks base method.
func (m *MockNwdafApp) CancelContext() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CancelContext")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// CancelContext indicates an expected call of CancelContext.
func (mr *MockNwdafAppMockRecorder) CancelContext() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"CancelContext",
		reflect.TypeOf((*MockNwdafApp)(nil).CancelContext),
	)
}

// Config mocks base method.
func (m *MockNwdafApp) Config() *factory.Config {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Config")
	ret0, _ := ret[0].(*factory.Config)
	return ret0
}

// Config indicates an expected call of Config.
func (mr *MockNwdafAppMockRecorder) Config() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"Config",
		reflect.TypeOf((*MockNwdafApp)(nil).Config),
	)
}

// Context mocks base method.
func (m *MockNwdafApp) Context() *nwdaf_context.NWDAFContext {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(*nwdaf_context.NWDAFContext)
	return ret0
}

// Context indicates an expected call of Context.
func (mr *MockNwdafAppMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"Context",
		reflect.TypeOf((*MockNwdafApp)(nil).Context),
	)
}

// Consumer mocks base method.
func (m *MockNwdafApp) Consumer() consumer.ConsumerAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Consumer")
	ret0, _ := ret[0].(consumer.ConsumerAPI)
	return ret0
}

// Consumer indicates an expected call of Consumer.
func (mr *MockNwdafAppMockRecorder) Consumer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"Consumer",
		reflect.TypeOf((*MockNwdafApp)(nil).Consumer),
	)
}

// MockConsumerAPI is a mock of consumer.ConsumerAPI interface.
type MockConsumerAPI struct {
	ctrl     *gomock.Controller
	recorder *MockConsumerAPIMockRecorder
}

// MockConsumerAPIMockRecorder is the mock recorder for MockConsumerAPI.
type MockConsumerAPIMockRecorder struct {
	mock *MockConsumerAPI
}

// NewMockConsumerAPI creates a new mock instance.
func NewMockConsumerAPI(ctrl *gomock.Controller) *MockConsumerAPI {
	mock := &MockConsumerAPI{ctrl: ctrl}
	mock.recorder = &MockConsumerAPIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConsumerAPI) EXPECT() *MockConsumerAPIMockRecorder {
	return m.recorder
}

// AdrfClient mocks base method.
func (m *MockConsumerAPI) AdrfClient() *consumer.AdrfClient {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AdrfClient")
	ret0, _ := ret[0].(*consumer.AdrfClient)
	return ret0
}

// AdrfClient indicates an expected call of AdrfClient.
func (mr *MockConsumerAPIMockRecorder) AdrfClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"AdrfClient",
		reflect.TypeOf((*MockConsumerAPI)(nil).AdrfClient),
	)
}

// SubscribeToSmf mocks base method.
func (m *MockConsumerAPI) SubscribeToSmf(
	smfEndpoint string,
	opts consumer.SmfSubscriptionOptions,
) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeToSmf", smfEndpoint, opts)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SubscribeToSmf indicates an expected call of SubscribeToSmf.
func (mr *MockConsumerAPIMockRecorder) SubscribeToSmf(
	smfEndpoint, opts any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SubscribeToSmf",
		reflect.TypeOf((*MockConsumerAPI)(nil).SubscribeToSmf),
		smfEndpoint,
		opts,
	)
}

// UnsubscribeFromSmf mocks base method.
func (m *MockConsumerAPI) UnsubscribeFromSmf(smfEndpoint, subscriptionID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnsubscribeFromSmf", smfEndpoint, subscriptionID)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnsubscribeFromSmf indicates an expected call of UnsubscribeFromSmf.
func (mr *MockConsumerAPIMockRecorder) UnsubscribeFromSmf(
	smfEndpoint, subscriptionID any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"UnsubscribeFromSmf",
		reflect.TypeOf((*MockConsumerAPI)(nil).UnsubscribeFromSmf),
		smfEndpoint,
		subscriptionID,
	)
}

// SubscribeToMtlf mocks base method.
func (m *MockConsumerAPI) SubscribeToMtlf(
	mtlfEndpoint string,
	opts consumer.MtlfSubscriptionOptions,
) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeToMtlf", mtlfEndpoint, opts)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SubscribeToMtlf indicates an expected call of SubscribeToMtlf.
func (mr *MockConsumerAPIMockRecorder) SubscribeToMtlf(
	mtlfEndpoint, opts any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SubscribeToMtlf",
		reflect.TypeOf((*MockConsumerAPI)(nil).SubscribeToMtlf),
		mtlfEndpoint,
		opts,
	)
}

// UnsubscribeFromMtlf mocks base method.
func (m *MockConsumerAPI) UnsubscribeFromMtlf(mtlfEndpoint, subscriptionID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnsubscribeFromMtlf", mtlfEndpoint, subscriptionID)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnsubscribeFromMtlf indicates an expected call of UnsubscribeFromMtlf.
func (mr *MockConsumerAPIMockRecorder) UnsubscribeFromMtlf(
	mtlfEndpoint, subscriptionID any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"UnsubscribeFromMtlf",
		reflect.TypeOf((*MockConsumerAPI)(nil).UnsubscribeFromMtlf),
		mtlfEndpoint,
		subscriptionID,
	)
}

// SetLogEnable mocks base method.
func (m *MockNwdafApp) SetLogEnable(enable bool) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetLogEnable", enable)
}

// SetLogEnable indicates an expected call of SetLogEnable.
func (mr *MockNwdafAppMockRecorder) SetLogEnable(enable any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SetLogEnable",
		reflect.TypeOf((*MockNwdafApp)(nil).SetLogEnable),
		enable,
	)
}

// SetLogLevel mocks base method.
func (m *MockNwdafApp) SetLogLevel(level string) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetLogLevel", level)
}

// SetLogLevel indicates an expected call of SetLogLevel.
func (mr *MockNwdafAppMockRecorder) SetLogLevel(level any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SetLogLevel",
		reflect.TypeOf((*MockNwdafApp)(nil).SetLogLevel),
		level,
	)
}

// SetReportCaller mocks base method.
func (m *MockNwdafApp) SetReportCaller(reportCaller bool) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetReportCaller", reportCaller)
}

// SetReportCaller indicates an expected call of SetReportCaller.
func (mr *MockNwdafAppMockRecorder) SetReportCaller(reportCaller any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SetReportCaller",
		reflect.TypeOf((*MockNwdafApp)(nil).SetReportCaller),
		reportCaller,
	)
}

// Start mocks base method.
func (m *MockNwdafApp) Start() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Start")
}

// Start indicates an expected call of Start.
func (mr *MockNwdafAppMockRecorder) Start() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"Start",
		reflect.TypeOf((*MockNwdafApp)(nil).Start),
	)
}

// Terminate mocks base method.
func (m *MockNwdafApp) Terminate() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Terminate")
}

// Terminate indicates an expected call of Terminate.
func (mr *MockNwdafAppMockRecorder) Terminate() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"Terminate",
		reflect.TypeOf((*MockNwdafApp)(nil).Terminate),
	)
}

// MockSmfServiceClient is a mock of consumer.SmfServiceClient interface.
type MockSmfServiceClient struct {
	ctrl     *gomock.Controller
	recorder *MockSmfServiceClientMockRecorder
}

// MockSmfServiceClientMockRecorder is the mock recorder for MockSmfServiceClient.
type MockSmfServiceClientMockRecorder struct {
	mock *MockSmfServiceClient
}

// NewMockSmfServiceClient creates a new mock instance.
func NewMockSmfServiceClient(ctrl *gomock.Controller) *MockSmfServiceClient {
	mock := &MockSmfServiceClient{ctrl: ctrl}
	mock.recorder = &MockSmfServiceClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSmfServiceClient) EXPECT() *MockSmfServiceClientMockRecorder {
	return m.recorder
}

// HTTPClient mocks base method.
func (m *MockSmfServiceClient) HTTPClient() *http.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "HTTPClient")
	ret0, _ := ret[0].(*http.Client)
	return ret0
}

// HTTPClient indicates an expected call of HTTPClient.
func (mr *MockSmfServiceClientMockRecorder) HTTPClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"HTTPClient",
		reflect.TypeOf((*MockSmfServiceClient)(nil).HTTPClient),
	)
}

// SubscribeToSmf mocks base method.
func (m *MockSmfServiceClient) SubscribeToSmf(
	smfEndpoint string,
	opts consumer.SmfSubscriptionOptions,
) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeToSmf", smfEndpoint, opts)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SubscribeToSmf indicates an expected call of SubscribeToSmf.
func (mr *MockSmfServiceClientMockRecorder) SubscribeToSmf(
	smfEndpoint, opts any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SubscribeToSmf",
		reflect.TypeOf((*MockSmfServiceClient)(nil).SubscribeToSmf),
		smfEndpoint,
		opts,
	)
}

// UnsubscribeFromSmf mocks base method.
func (m *MockSmfServiceClient) UnsubscribeFromSmf(smfEndpoint string, subscriptionID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnsubscribeFromSmf", smfEndpoint, subscriptionID)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnsubscribeFromSmf indicates an expected call of UnsubscribeFromSmf.
func (mr *MockSmfServiceClientMockRecorder) UnsubscribeFromSmf(
	smfEndpoint, subscriptionID any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"UnsubscribeFromSmf",
		reflect.TypeOf((*MockSmfServiceClient)(nil).UnsubscribeFromSmf),
		smfEndpoint,
		subscriptionID,
	)
}
