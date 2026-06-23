package processor

import (
	"context"
	"net/http"
	"reflect"

	gomock "go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
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

// Consumer mocks base method.
func (m *MockNwdafApp) Consumer() *consumer.Consumer {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Consumer")
	ret0, _ := ret[0].(*consumer.Consumer)
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
