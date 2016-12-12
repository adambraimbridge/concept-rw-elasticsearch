package main

import (
	"encoding/json"
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v3"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthDetailsHealthyCluster(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__health-details", nil)
	if err != nil {
		t.Fatal(err)
	}

	var dummyEsHealthService esHealthServiceI = &dummyEsHealthService{healthy: true, returnsError: false}
	healthService := newHealthService(&dummyEsHealthService)

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
}

func TestHealthDetailsReturnsError(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__health-details", nil)
	if err != nil {
		t.Fatal(err)
	}
	var dummyEsHealthService esHealthServiceI = &dummyEsHealthService{returnsError: true}
	healthService := newHealthService(&dummyEsHealthService)

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
		t.Errorf("Response body should be empty")
	}
}

func TestGoodToGoHealthyCluster(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}
	var dummyEsHealthService esHealthServiceI = &dummyEsHealthService{returnsError: true}
	healthService := newHealthService(&dummyEsHealthService)

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
		t.Errorf("Response body should be empty")
	}
}

func TestGoodToGoUnhealthyCluster(t *testing.T) {

	//create a request to pass to our handler
	req, err := http.NewRequest("GET", "/__gtg", nil)
	if err != nil {
		t.Fatal(err)
	}
	var dummyEsHealthService esHealthServiceI = &dummyEsHealthService{healthy: true, returnsError: false}
	healthService := newHealthService(&dummyEsHealthService)

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
		t.Errorf("Response body should be empty")
	}
}

func TestHealthServiceConnectivityChecker(t *testing.T) {

	var dummyEsHealthService esHealthServiceI = &dummyEsHealthService{healthy: true, returnsError: false}
	healthService := newHealthService(&dummyEsHealthService)
	message, err := healthService.connectivityChecker()

	assert.Equal(t, message, "Successfully connected to the cluster")
	assert.Equal(t, err, nil)

}

func TestHealthServiceConnectivityCheckerForFailedConnection(t *testing.T) {

	var dummyEsHealthService esHealthServiceI = &dummyEsHealthService{returnsError: true}
	healthService := newHealthService(&dummyEsHealthService)
	message, err := healthService.connectivityChecker()

	assert.Equal(t, message, "Could not connect to elasticsearch")
	assert.NotNil(t, err)

}

type dummyEsHealthService struct {
	healthy      bool
	returnsError bool
}

func (service dummyEsHealthService) getClusterHealth() (*elastic.ClusterHealthResponse, error) {
	if service.returnsError {
		return nil, errors.New("Request ended up in retuning some internal error")
	}

	if service.healthy {
		return &elastic.ClusterHealthResponse{Status: "green"}, nil
	} else {
		return &elastic.ClusterHealthResponse{Status: "red"}, nil
	}
}
