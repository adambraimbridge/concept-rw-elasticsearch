package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"context"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
	status "github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"gopkg.in/olivere/elastic.v5"
)

var (
	happyESCluster   = &elastic.ClusterHealthResponse{Status: "green"}
	unhappyESCluster = &elastic.ClusterHealthResponse{Status: "red"}
)

func TestHealthDetailsHealthyCluster(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__health-details", nil)
	if err != nil {
		t.Fatal(err)
	}

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(happyESCluster, nil)
	healthService := NewHealthService(esService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthDetails)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	if contentType := rr.HeaderMap.Get("Content-Type"); contentType != "application/json" {
		t.Errorf("handler returned wrong content type: got %v want %v",
			contentType, "application/json")
	}

	var respObject *elastic.ClusterHealthResponse
	err = json.Unmarshal(rr.Body.Bytes(), &respObject)
	if err != nil {
		t.Errorf("Unmarshalling request response failed. %v", err)
	}
	if respObject.Status != "green" {
		t.Errorf("Cluster status it is not as expected, got %v want %v", respObject.Status, "green")
	}

	esService.AssertExpectations(t)
}

func TestHealthDetailsReturnsError(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__health-details", nil)
	if err != nil {
		t.Fatal(err)
	}

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(unhappyESCluster, errors.New("computer says no"))
	healthService := NewHealthService(esService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthDetails)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Series of verifications:
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	if contentType := rr.HeaderMap.Get("Content-Type"); contentType != "application/json" {
		t.Errorf("handler returned wrong content type: got %v want %v",
			contentType, "application/json")
	}

	if rr.Body.Bytes() != nil {
		t.Error("Response body should be empty")
	}

	esService.AssertExpectations(t)
}

func TestGoodToGoUnhealthyESCluster(t *testing.T) {
	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}
	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(unhappyESCluster, errors.New("computer says no"))
	healthService := NewHealthService(esService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(status.NewGoodToGoHandler(healthService.GTG))

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Series of verifications:
	if status := rr.Code; status != http.StatusServiceUnavailable {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusServiceUnavailable)
	}

	assert.Equal(t, "computer says no", rr.Body.String(), "GTG response body")

	esService.AssertExpectations(t)
}

func TestHappyGoodToGo(t *testing.T) {
	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(happyESCluster, nil)
	healthService := NewHealthService(esService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(status.NewGoodToGoHandler(healthService.GTG))

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Series of verifications:
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	assert.Equal(t, "OK", rr.Body.String(), "GTG response body")
	esService.AssertExpectations(t)
}

func TestHappyHealthCheck(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(happyESCluster, nil)
	esService.On("IsIndexReadOnly").Return(false, "indexName", nil)
	healthService := NewHealthService(esService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response Body should be consistent")

	for _, check := range checks {
		assert.True(t, check.Ok)
	}

	esService.AssertExpectations(t)
}

func TestHealthCheckUnhealthyESCluster(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(unhappyESCluster, nil)
	esService.On("IsIndexReadOnly").Return(false, "indexName", nil)
	healthService := NewHealthService(esService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response Body should be consistent")

	for _, check := range checks {
		if check.ID == "check-elasticsearch-cluster-health" {
			assert.False(t, check.Ok)
		} else {
			assert.True(t, check.Ok)
		}
	}

	esService.AssertExpectations(t)

}

func TestHealthCheckNoESClusterConnection(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(unhappyESCluster, errors.New("computer says no"))
	esService.On("IsIndexReadOnly").Return(false, "indexName", nil)
	healthService := NewHealthService(esService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response Body should be consistent")

	for _, check := range checks {
		if check.ID == "check-elasticsearch-cluster-health" || check.ID == "check-connectivity-to-elasticsearch-cluster" {
			assert.False(t, check.Ok)
		} else {
			assert.True(t, check.Ok)
		}
	}

	esService.AssertExpectations(t)

}

func TestHealthCheckReadOnlyIndex(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esService := new(EsServiceMock)
	esService.On("GetClusterHealth").Return(happyESCluster, nil)
	esService.On("IsIndexReadOnly").Return(true, "indexName", nil)

	healthService := NewHealthService(esService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response Body should be consistent")

	for _, check := range checks {
		if check.ID == "check-elasticsearch-index-writeable" {
			assert.False(t, check.Ok)
		} else {
			assert.True(t, check.Ok)
		}
	}

	esService.AssertExpectations(t)

}

type EsServiceMock struct {
	mock.Mock
}

func (m *EsServiceMock) LoadData(ctx context.Context, conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	args := m.Called(ctx, conceptType, uuid, payload)
	return args.Get(0).(*elastic.IndexResponse), args.Error(1)
}

func (m *EsServiceMock) ReadData(conceptType string, uuid string) (*elastic.GetResult, error) {
	args := m.Called(conceptType, uuid)
	return args.Get(0).(*elastic.GetResult), args.Error(1)
}

func (m *EsServiceMock) DeleteData(ctx context.Context, conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	args := m.Called(ctx, conceptType, uuid)
	return args.Get(0).(*elastic.DeleteResponse), args.Error(1)
}

func (m *EsServiceMock) LoadBulkData(conceptType string, uuid string, payload interface{}) {
	m.Called(conceptType, uuid, payload)
}

func (m *EsServiceMock) PatchUpdateDataWithMetrics(ctx context.Context, conceptType string, uuid string, payload *service.MetricsPayload) {
	m.Called(ctx, conceptType, uuid, payload)
}

func (m *EsServiceMock) CleanupData(ctx context.Context, concept service.Concept) {
	m.Called(ctx, concept)
}

func (m *EsServiceMock) CloseBulkProcessor() error {
	args := m.Called()
	return args.Error(0)
}

func (m *EsServiceMock) GetClusterHealth() (*elastic.ClusterHealthResponse, error) {
	args := m.Called()
	return args.Get(0).(*elastic.ClusterHealthResponse), args.Error(1)
}

func (m *EsServiceMock) IsIndexReadOnly() (bool, string, error) {
	args := m.Called()
	return args.Bool(0), args.String(1), args.Error(2)
}

func parseHealthcheck(healthcheckJSON string) ([]fthealth.CheckResult, error) {
	result := &struct {
		Checks []fthealth.CheckResult `json:"checks"`
	}{}

	err := json.Unmarshal([]byte(healthcheckJSON), result)
	return result.Checks, err
}

func (m *EsServiceMock) GetAllIds(ctx context.Context) chan service.EsIDTypePair {
	args := m.Called()
	return args.Get(0).(chan service.EsIDTypePair)
}
