package resources

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
)

var (
	testError = errors.New("test error")
)

func TestCreateNewESWriter(t *testing.T) {
	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}

	allowedTypes := []string{"organisations", "genres"}
	writerService := NewHandler(dummyEsService, dummyAuthorService, allowedTypes)
	assert.True(t, writerService.allowedConceptTypes["organisations"])
	assert.True(t, writerService.allowedConceptTypes["genres"])
	assert.False(t, writerService.allowedConceptTypes["something else"])
}

func TestCreateNewESWriterWithEmptyWhitelist(t *testing.T) {
	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	allowedTypes := []string{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, allowedTypes)
	assert.Equal(t, 0, len(writerService.allowedConceptTypes))
}

func TestLoadData(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations", "people", "genres"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{returnsError: testError}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.LoadBulkData).Methods("PUT")
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

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.LoadBulkData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestLoadBulkDataAccepted(t *testing.T) {

	payload := `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`
	req, err := http.NewRequest("PUT", "/bulk/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(payload)))
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	dummyEsService := &dummyEsService{}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.LoadBulkData).Methods("PUT")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestReadData(t *testing.T) {
	req, err := http.NewRequest("GET", "/genres/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	esModel := &service.EsConceptModel{
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
	dummyEsService := &dummyEsService{found: true, source: &rawmsg}
	dummyAuthorService := &dummyAuthorService{isAuthor: "false", authorIds: []service.AuthorUUID{}}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"genres"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.ReadData).Methods("GET")
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

	var respObject *service.EsConceptModel
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

	dummyEsService := &dummyEsService{found: false}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.ReadData).Methods("GET")
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

	dummyEsService := &dummyEsService{returnsError: testError}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.ReadData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

func TestReadDataEsServerUnavailable(t *testing.T) {
	req, err := http.NewRequest("GET", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	dummyEsService := &dummyEsService{returnsError: service.ErrNoElasticClient}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.ReadData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	assert.Equal(t, http.StatusServiceUnavailable, rr.Code, "HTTP status")
}

func TestDeleteData(t *testing.T) {

	req, err := http.NewRequest("DELETE", "/organisations/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()

	dummyEsService := &dummyEsService{found: true}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.DeleteData).Methods("DELETE")
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

	dummyEsService := &dummyEsService{found: false}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.DeleteData).Methods("DELETE")
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

	dummyEsService := &dummyEsService{returnsError: testError}
	dummyAuthorService := &dummyAuthorService{}
	writerService := NewHandler(dummyEsService, dummyAuthorService, []string{"organisations"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.DeleteData).Methods("DELETE")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusInternalServerError {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusInternalServerError)
	}

	assert.Nil(t, rr.Body.Bytes(), "Response body should be empty")
}

type dummyEsService struct {
	returnsError error
	found        bool
	source       *json.RawMessage
}

func (service *dummyEsService) LoadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	if service.returnsError != nil {
		return nil, service.returnsError
	} else {
		return &elastic.IndexResponse{}, nil
	}
}

func (service *dummyEsService) ReadData(conceptType string, uuid string) (*elastic.GetResult, error) {
	if service.returnsError != nil {
		return nil, service.returnsError
	} else {
		return &elastic.GetResult{Found: service.found, Source: service.source}, nil
	}
}

func (service *dummyEsService) DeleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	if service.returnsError != nil {
		return nil, service.returnsError
	} else {
		return &elastic.DeleteResponse{Found: service.found}, nil
	}
}

func (service *dummyEsService) LoadBulkData(conceptType string, uuid string, payload interface{}) {

}

func (service *dummyEsService) CloseBulkProcessor() error {
	if service.returnsError != nil {
		return service.returnsError
	} else {
		return nil
	}
}

type dummyAuthorService struct {
	isAuthor  string
	authorIds []service.AuthorUUID
	gtg       error
}

func (service *dummyAuthorService) LoadAuthorIdentifiers() error {
	return nil
}

func (service *dummyAuthorService) IsFTAuthor(UUID string) string {
	return service.isAuthor
}

func (service *dummyAuthorService) IsGTG() error {
	return service.gtg
}
