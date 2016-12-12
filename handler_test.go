package main

import (
	"bytes"
	"encoding/json"
	"github.com/docker/docker/pkg/testutil/assert"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"gopkg.in/olivere/elastic.v3"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLoadData(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadDataBadRequest(t *testing.T) {

	payload := `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadDataBadRequestForEmptyType(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadDataBadRequestForEmptyPrefLabel(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadDataEsClientServerErrors(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: true}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadDataIncorrectPayload(t *testing.T) {

	payload := `{wrong data}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadBulkDataIncorrectPayload(t *testing.T) {

	payload := `{wrong data}`
	req, err := http.NewRequest("PUT", "/bulk/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.loadBulkData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestLoadBulkDataBadRequest(t *testing.T) {

	payload := `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/bulk/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.loadBulkData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestReadData(t *testing.T) {
	req, err := http.NewRequest("GET", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	esModel := &esConceptModel{
		Id:         "http://api.ft.com/things/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		ApiUrl:     "http://api.ft.com/things/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		PrefLabel:  "Market Report",
		Types:      []string{"http://www.ft.com/ontology/core/Thing", "http://www.ft.com/ontology/concept/Concept", "http://www.ft.com/ontology/classification/Classification", "http://www.ft.com/ontology/Genre"},
		DirectType: "http://www.ft.com/ontology/Genre",
	}
	rawModel, err := json.Marshal(esModel)
	if err != nil {
		t.Fatal(err)
	}

	var rawmsg json.RawMessage = json.RawMessage(rawModel)
	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: true, source: &rawmsg}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.readData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	if contentType := rr.HeaderMap.Get("Content-Type"); contentType != "application/json" {
		t.Errorf("handler returned wrong content type: got %v want %v",
			contentType, "application/json")
	}

	var respObject *esConceptModel
	err = json.Unmarshal(rr.Body.Bytes(), &respObject)
	if err != nil {
		t.Errorf("Unmarshalling request response failed. %v", err)
	}

	assert.DeepEqual(t, respObject, esModel)

}

func TestReadDataNotFound(t *testing.T) {
	req, err := http.NewRequest("GET", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.readData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNotFound)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestReadDataEsServerError(t *testing.T) {
	req, err := http.NewRequest("GET", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: true}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.readData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestDeleteData(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: true}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.deleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestDeleteDataNotFound(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: false}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.deleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNotFound)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

func TestDeleteDataEsServerError(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: true}
	writerService := newESWriter(&dummyEsService)

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.deleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	if rr.Body.Bytes() != nil {
		t.Errorf("Response body should be empty")
	}
}

type dummyEsService struct {
	returnsError bool
	found        bool
	source       *json.RawMessage
}

func (service *dummyEsService) loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	if service.returnsError {
		return nil, errors.New("Error")
	} else {
		return &elastic.IndexResponse{}, nil
	}
}

func (service *dummyEsService) readData(conceptType string, uuid string) (*elastic.GetResult, error) {
	if service.returnsError {
		return nil, errors.New("Error")
	} else {
		return &elastic.GetResult{Found: service.found, Source: service.source}, nil
	}
}

func (service *dummyEsService) deleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	if service.returnsError {
		return nil, errors.New("Error")
	} else {
		return &elastic.DeleteResponse{Found: service.found}, nil
	}
}

func (service *dummyEsService) loadBulkData(conceptType string, uuid string, payload interface{}) {

}

func (service *dummyEsService) closeBulkProcessor() error {
	if service.returnsError {
		return errors.New("Error")
	} else {
		return nil
	}
}
