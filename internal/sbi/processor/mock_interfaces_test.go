package processor

import (
	"context"
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

// SubscribeToMtlf mocks base method.
func (m *MockConsumerAPI) SubscribeToMtlf(
	ctx context.Context,
	mtlfEndpoint string,
	opts consumer.MtlfSubscriptionOptions,
) (string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeToMtlf", ctx, mtlfEndpoint, opts)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// SubscribeToMtlf indicates an expected call of SubscribeToMtlf.
func (mr *MockConsumerAPIMockRecorder) SubscribeToMtlf(
	ctx, mtlfEndpoint, opts any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"SubscribeToMtlf",
		reflect.TypeOf((*MockConsumerAPI)(nil).SubscribeToMtlf),
		ctx,
		mtlfEndpoint,
		opts,
	)
}

// UnsubscribeFromMtlf mocks base method.
func (m *MockConsumerAPI) UnsubscribeFromMtlf(ctx context.Context, mtlfEndpoint, subscriptionID string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UnsubscribeFromMtlf", ctx, mtlfEndpoint, subscriptionID)
	ret0, _ := ret[0].(error)
	return ret0
}

// UnsubscribeFromMtlf indicates an expected call of UnsubscribeFromMtlf.
func (mr *MockConsumerAPIMockRecorder) UnsubscribeFromMtlf(
	ctx, mtlfEndpoint, subscriptionID any,
) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(
		mr.mock,
		"UnsubscribeFromMtlf",
		reflect.TypeOf((*MockConsumerAPI)(nil).UnsubscribeFromMtlf),
		ctx,
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
