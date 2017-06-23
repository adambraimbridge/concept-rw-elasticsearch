package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	fthealth "github.com/Financial-Times/go-fthealth/v1_1"
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

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(happyESCluster, nil)
	authorService := new(AuthorServiceMock)
	healthService := NewHealthService(esHealthService, authorService)

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

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestHealthDetailsReturnsError(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__health-details", nil)
	if err != nil {
		t.Fatal(err)
	}

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(unhappyESCluster, errors.New("computer says no"))
	authorService := new(AuthorServiceMock)
	healthService := NewHealthService(esHealthService, authorService)

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

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestGoodToGoUnhealthyESCluster(t *testing.T) {
	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}
	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(unhappyESCluster, errors.New("computer says no"))
	authorService := new(AuthorServiceMock)
	healthService := NewHealthService(esHealthService, authorService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.GoodToGo)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Series of verifications:
	if status := rr.Code; status != http.StatusServiceUnavailable {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusServiceUnavailable)
	}

	if rr.Body.Bytes() != nil {
		t.Error("Response body should be empty")
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestHappyGoodToGo(t *testing.T) {
	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(happyESCluster, nil)
	authorService := new(AuthorServiceMock)
	authorService.On("IsGTG").Return(nil)
	healthService := NewHealthService(esHealthService, authorService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.GoodToGo)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Series of verifications:
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	if rr.Body.Bytes() != nil {
		t.Error("Response body should be empty")
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestGoodToGoUnhealthyV1AuthorsTransformer(t *testing.T) {
	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}
	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(happyESCluster, nil)
	authorService := new(AuthorServiceMock)
	authorService.On("IsGTG").Return(errors.New("computer says no"))
	healthService := NewHealthService(esHealthService, authorService)

	//create a responseRecorder
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.GoodToGo)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Series of verifications:
	if status := rr.Code; status != http.StatusServiceUnavailable {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusServiceUnavailable)
	}

	if rr.Body.Bytes() != nil {
		t.Error("Response body should be empty")
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestHappyHealthCheck(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(happyESCluster, nil)
	authorService := new(AuthorServiceMock)
	authorService.On("IsGTG").Return(nil)
	healthService := NewHealthService(esHealthService, authorService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response BOdy should be consistent")

	for _, check := range checks {
		assert.True(t, check.Ok)
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestHealthCheckUnhealthyESCluster(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(unhappyESCluster, nil)
	authorService := new(AuthorServiceMock)
	authorService.On("IsGTG").Return(nil)
	healthService := NewHealthService(esHealthService, authorService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response BOdy should be consistent")

	for _, check := range checks {
		if check.ID == "check-elasticsearch-cluster-health" {
			assert.False(t, check.Ok)
		} else {
			assert.True(t, check.Ok)
		}
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestHealthCheckNoESClusterConnection(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(unhappyESCluster, errors.New("computer says no"))
	authorService := new(AuthorServiceMock)
	authorService.On("IsGTG").Return(nil)
	healthService := NewHealthService(esHealthService, authorService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response BOdy should be consistent")

	for _, check := range checks {
		if check.ID == "check-elasticsearch-cluster-health" || check.ID == "check-connectivity-to-elasticsearch-cluster" {
			assert.False(t, check.Ok)
		} else {
			assert.True(t, check.Ok)
		}
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

func TestHealthCheckV1AuthorsTransformNotGTG(t *testing.T) {
	req, err := http.NewRequest("GET", "/__health", nil)
	assert.NoError(t, err, "HTTP request to healthcheck should be consistent")

	esHealthService := new(EsHealthServiceMock)
	esHealthService.On("GetClusterHealth").Return(happyESCluster, nil)
	authorService := new(AuthorServiceMock)
	authorService.On("IsGTG").Return(errors.New("computer says no"))
	healthService := NewHealthService(esHealthService, authorService)

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(healthService.HealthCheckHandler())

	handler.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code, "HealthCheck should return HTTP 200 OK")

	checks, err := parseHealthcheck(rr.Body.String())
	assert.NoError(t, err, "HealthCheck Response BOdy should be consistent")

	for _, check := range checks {
		if check.ID == "check-connectivity-to-v1-authors-transformer" {
			assert.False(t, check.Ok)
		} else {
			assert.True(t, check.Ok)
		}
	}

	esHealthService.AssertExpectations(t)
	authorService.AssertExpectations(t)
}

type EsHealthServiceMock struct {
	mock.Mock
}

func (m *EsHealthServiceMock) GetClusterHealth() (*elastic.ClusterHealthResponse, error) {
	args := m.Called()
	return args.Get(0).(*elastic.ClusterHealthResponse), args.Error(1)
}

type AuthorServiceMock struct {
	mock.Mock
}

func (m *AuthorServiceMock) LoadAuthorIdentifiers() error {
	args := m.Called()
	return args.Error(0)
}

func (m *AuthorServiceMock) IsFTAuthor(uuid string) string {
	args := m.Called(uuid)
	return args.String(0)
}

func (m *AuthorServiceMock) IsGTG() error {
	args := m.Called()
	return args.Error(0)
}

func parseHealthcheck(healthcheckJSON string) ([]fthealth.CheckResult, error) {
	result := &struct {
		Checks []fthealth.CheckResult `json:"checks"`
	}{}

	err := json.Unmarshal([]byte(healthcheckJSON), result)
	return result.Checks, err
}
