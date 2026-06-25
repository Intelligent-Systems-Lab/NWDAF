package processor

import (
	"net/http"
	"reflect"

	gomock "go.uber.org/mock/gomock"

	"github.com/free5gc/nwdaf/internal/sbi/consumer"
)

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
func (m *MockConsumerAPI) AdrfClient() consumer.AdrfServiceAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AdrfClient")
	ret0, _ := ret[0].(consumer.AdrfServiceAPI)
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

// MlClient mocks base method.
func (m *MockConsumerAPI) MlClient() consumer.MlServiceAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MlClient")
	ret0, _ := ret[0].(consumer.MlServiceAPI)
	return ret0
}

// MlClient indicates an expected call of MlClient.
func (mr *MockConsumerAPIMockRecorder) MlClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"MlClient",
		reflect.TypeOf((*MockConsumerAPI)(nil).MlClient),
	)
}

// DaisyClient mocks base method.
func (m *MockConsumerAPI) DaisyClient() consumer.DaisyServiceAPI {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DaisyClient")
	ret0, _ := ret[0].(consumer.DaisyServiceAPI)
	return ret0
}

// DaisyClient indicates an expected call of DaisyClient.
func (mr *MockConsumerAPIMockRecorder) DaisyClient() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"DaisyClient",
		reflect.TypeOf((*MockConsumerAPI)(nil).DaisyClient),
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
