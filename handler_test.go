package main

import (
	"bytes"
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v3"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
)

func TestCreateNewESWriter(t *testing.T) {
	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	allowedTypes := []string{"organisations", "genres"}
	writerService := newESWriter(&dummyEsService, allowedTypes)
	assert.True(t, writerService.allowedConceptTypes["organisations"])
	assert.True(t, writerService.allowedConceptTypes["genres"])
	assert.False(t, writerService.allowedConceptTypes["something else"])
}

func TestCreateNewESWriterWithEmptyWhitelist(t *testing.T) {
	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	allowedTypes := []string{}
	writerService := newESWriter(&dummyEsService, allowedTypes)
	assert.Equal(t, 0, len(writerService.allowedConceptTypes))
}

func TestLoadData(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadDataBadRequest(t *testing.T) {

	payload := `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadDataBadRequestForUnsupportedType(t *testing.T) {

	payload := `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisation/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations", "people", "genres"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// For Bad Request the status code should be 400 - but for unsupported concept types the writer returns 200 - without further processing
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadDataBadRequestForEmptyType(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadDataBadRequestForEmptyPrefLabel(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadDataEsClientServerErrors(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: true}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadDataIncorrectPayload(t *testing.T) {

	payload := `{wrong data}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.loadData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadBulkDataIncorrectPayload(t *testing.T) {

	payload := `{wrong data}`
	req, err := http.NewRequest("PUT", "/bulk/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.loadBulkData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadBulkDataBadRequest(t *testing.T) {

	payload := `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/bulk/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.loadBulkData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestReadData(t *testing.T) {
	req, err := http.NewRequest("GET", "/genres/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
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
	writerService := newESWriter(&dummyEsService, []string{"genres"})

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

	assert.True(t, reflect.DeepEqual(respObject, esModel))

}

func TestReadDataNotFound(t *testing.T) {
	req, err := http.NewRequest("GET", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.readData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNotFound)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestReadDataEsServerError(t *testing.T) {
	req, err := http.NewRequest("GET", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: true}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.readData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestDeleteData(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: true}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.deleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestDeleteDataNotFound(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: false, found: false}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.deleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusNotFound {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusNotFound)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestDeleteDataEsServerError(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	var dummyEsService esServiceI = &dummyEsService{returnsError: true}
	writerService := newESWriter(&dummyEsService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.deleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
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
