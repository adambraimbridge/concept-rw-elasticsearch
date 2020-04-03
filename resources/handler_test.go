package resources

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"context"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	"github.com/Financial-Times/go-logger"
	tid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	testLog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v5"
)

var (
	errTest = errors.New("test error")
)

func init() {
	logger.InitLogger("test-concept-rw-elasticsearch", "debug")
}

func TestCreateNewESWriter(t *testing.T) {
	dummyEsService := &dummyEsService{}

	allowedTypes := []string{"organisations", "genres"}
	writerService := NewHandler(dummyEsService, allowedTypes)
	assert.True(t, writerService.allowedConceptTypes["organisations"])
	assert.True(t, writerService.allowedConceptTypes["genres"])
	assert.False(t, writerService.allowedConceptTypes["something else"])
}

func TestCreateNewESWriterWithEmptyWhitelist(t *testing.T) {
	dummyEsService := &dummyEsService{}
	var allowedTypes []string
	writerService := NewHandler(dummyEsService, allowedTypes)
	assert.Equal(t, 0, len(writerService.allowedConceptTypes))
}

func TestLoadData(t *testing.T) {
	testCases := []struct {
		name    string
		path    string
		payload string
		status  int
		msg     string
		noop    bool
	}{
		{
			name:    "Successful write",
			payload: `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`,
			status:  http.StatusOK,
			msg:     `{"message":"Concept written successfully"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Successful aggregate model write",
			payload: `{"prefUUID":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url","sourceRepresentations":[{"uuid":"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966","prefLabel":"TMEs PrefLabel","type":"Brand","authority":"TME","authorityValue":"745212"},{"uuid":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","authority":"Smartlogic","authorityValue":"123456789","lastModifiedEpoch":1498127042,"strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url"}]}`,
			status:  http.StatusOK,
			msg:     `{"message":"Concept written successfully"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Model dropped",
			payload: `{"prefUUID":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url","sourceRepresentations":[{"uuid":"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966","prefLabel":"TMEs PrefLabel","type":"Brand","authority":"TME","authorityValue":"745212"},{"uuid":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","authority":"Smartlogic","authorityValue":"123456789","lastModifiedEpoch":1498127042,"strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url"}]}`,
			status:  http.StatusNotModified,
			msg:     `{"message":"Concept dropped"}`,
			noop:    true,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Path contains different uuid to body",
			payload: `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Provided path UUID does not match request body"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Path contains unsupported concept type",
			payload: `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`,
			status:  http.StatusNotFound,
			msg:     `{"message":"Unsupported or invalid concept type"}`,
			path:    "/invalid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Body contains empty type",
			payload: `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report"}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Invalid or incomplete concept model"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Body contains empty prefLabel",
			payload: `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"type":"Genre"}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Invalid or incomplete concept model"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Path contains different uuid to aggregate model body",
			payload: `{"prefUUID":"different-uuid","prefLabel":"Smartlogics Brands PrefLabel","strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url","sourceRepresentations":[{"uuid":"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966","prefLabel":"TMEs PrefLabel","type":"Brand","authority":"TME","authorityValue":"745212"},{"uuid":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","authority":"Smartlogic","authorityValue":"123456789","lastModifiedEpoch":1498127042,"strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url"}]}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Provided path UUID does not match request body"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Aggregate model body contains empty type",
			payload: `{"prefUUID":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","prefLabel":"Smartlogics Brands PrefLabel","strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url","sourceRepresentations":[{"uuid":"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966","prefLabel":"TMEs PrefLabel","type":"Brand","authority":"TME","authorityValue":"745212"},{"uuid":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","authority":"Smartlogic","authorityValue":"123456789","lastModifiedEpoch":1498127042,"strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url"}]}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Invalid or incomplete concept model"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Aggregate model body contains empty prefLabel",
			payload: `{"prefUUID":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","type":"Brands","strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url","sourceRepresentations":[{"uuid":"4ebbd9c4-3bb7-4d18-a14c-4c45aac5d966","prefLabel":"TMEs PrefLabel","type":"Brand","authority":"TME","authorityValue":"745212"},{"uuid":"56388858-38d6-4dfc-a001-506394259b51","prefLabel":"Smartlogics Brands PrefLabel","type":"Brand","authority":"Smartlogic","authorityValue":"123456789","lastModifiedEpoch":1498127042,"strapline":"Some strapline","descriptionXML":"Some description","_imageUrl":"Some image url"}]}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Invalid or incomplete concept model"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Body contains invalid json",
			payload: `{wrong data}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Request body is not in the expected concept model format"}`,
			path:    "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Bulk request successful",
			payload: `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`,
			status:  http.StatusOK,
			msg:     `{"message":"Concept written successfully"}`,
			path:    "/bulk/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Bulk request body contains invalid json",
			payload: `{wrong data}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Request body is not in the expected concept model format"}`,
			path:    "/bulk/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Bulk request unsupported concept type",
			payload: `{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`,
			status:  http.StatusNotFound,
			msg:     `{"message":"Unsupported or invalid concept type"}`,
			path:    "/bulk/invalid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Bulk path contains different uuid to body",
			payload: `{"uuid":"different-uuid","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Provided path UUID does not match request body"}`,
			path:    "/bulk/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Metrics are written successfully",
			payload: `{"metrics":{"annotationsCount":796, "prevWeekAnnotationsCount": 79}}`,
			status:  http.StatusOK,
			msg:     `{"message":"Concept updated with metrics successfully"}`,
			path:    "/metrics/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Metrics are not written for invalid type",
			payload: `{"metrics":{"annotationsCount": 796, "prevWeekAnnotationsCount": 79}`,
			status:  http.StatusNotFound,
			msg:     `{"message":"Unsupported or invalid concept type"}`,
			path:    "/metrics/invalid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
		{
			name:    "Metrics are only written if they are supplied correctly",
			payload: `{"somethingDodgy":{"annotationsCount": 796, "prevWeekAnnotationsCount": 79}}`,
			status:  http.StatusBadRequest,
			msg:     `{"message":"Please supply metrics as a JSON object with a single property 'metrics'"}`,
			path:    "/metrics/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest("PUT", tc.path, bytes.NewReader([]byte(tc.payload)))
			require.NoError(t, err, `Current test "%v"`, tc.name)

			rr := httptest.NewRecorder()

			dummyEsService := &dummyEsService{noop: tc.noop}
			writerService := NewHandler(dummyEsService, []string{"valid-type"})

			servicesRouter := mux.NewRouter()
			servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
			servicesRouter.HandleFunc("/bulk/{concept-type}/{id}", writerService.LoadBulkData).Methods("PUT")
			servicesRouter.HandleFunc("/metrics/{concept-type}/{id}", writerService.LoadMetrics).Methods("PUT")
			servicesRouter.ServeHTTP(rr, req)

			assert.Equal(t, tc.status, rr.Code, `Current test "%v"`, tc.name)
			assert.JSONEq(t, tc.msg, rr.Body.String(), `Current test "%v"`, tc.name)
		})
	}
}

func TestLoadDataEsClientServerErrors(t *testing.T) {
	testCases := []struct {
		err    error
		status int
		msg    string
	}{
		{
			err:    errTest,
			status: http.StatusInternalServerError,
			msg:    `{"message":"Failed to write data to ES"}`,
		},
		{
			err:    service.ErrNoElasticClient,
			status: http.StatusServiceUnavailable,
			msg:    `{"message":"ES unavailable"}`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.err.Error(), func(t *testing.T) {
			req, err := http.NewRequest("PUT", "/valid-type/8ff7dfef-0330-3de0-b37a-2d6aa9c98580", bytes.NewReader([]byte(`{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`)))
			require.NoError(t, err)

			rr := httptest.NewRecorder()

			dummyEsService := &dummyEsService{returnsError: tc.err}
			writerService := NewHandler(dummyEsService, []string{"valid-type"})

			servicesRouter := mux.NewRouter()
			servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.LoadData).Methods("PUT")
			servicesRouter.ServeHTTP(rr, req)

			assert.Equal(t, tc.status, rr.Code)
			assert.JSONEq(t, tc.msg, rr.Body.String())
		})
	}
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

	rawmsg := json.RawMessage(rawModel)
	dummyEsService := &dummyEsService{found: true, source: &rawmsg}
	writerService := NewHandler(dummyEsService, []string{"genres"})

	servicesRouter := mux.NewRouter()
	servicesRouter.HandleFunc("/{concept-type}/{id}", writerService.ReadData).Methods("GET")
	servicesRouter.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v", status, http.StatusOK)
	}

	if contentType := rr.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("handler returned wrong content type: got %v want %v", contentType, "application/json")
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
	writerService := NewHandler(dummyEsService, []string{"organisations"})

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

	dummyEsService := &dummyEsService{returnsError: errTest}
	writerService := NewHandler(dummyEsService, []string{"organisations"})

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
	writerService := NewHandler(dummyEsService, []string{"organisations"})

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
	writerService := NewHandler(dummyEsService, []string{"organisations"})

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
	writerService := NewHandler(dummyEsService, []string{"organisations"})

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

	dummyEsService := &dummyEsService{returnsError: errTest}
	writerService := NewHandler(dummyEsService, []string{"organisations"})

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

func TestProcessConceptModelWithoutTransactionID(t *testing.T) {
	hook := testLog.NewLocal(logger.Logger())
	testUUID := "8ff7dfef-0330-3de0-b37a-2d6aa9c98580"
	testBody := []byte(`{"uuid":"8ff7dfef-0330-3de0-b37a-2d6aa9c98580","alternativeIdentifiers":{"TME":["Mg==-R2VucmVz"],"uuids":["8ff7dfef-0330-3de0-b37a-2d6aa9c98580"]},"prefLabel":"Market Report","type":"Genre"}`)

	_, payload, err := processConceptModel(context.Background(), testUUID, "genres", testBody)
	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.NotEmpty(t, payload.(*service.EsConceptModel).PublishReference)

	assert.Equal(t, log.WarnLevel, hook.LastEntry().Level)
	assert.Equal(t, "Transaction ID not found to process concept model. Generated new transaction ID", hook.LastEntry().Message)
	assert.Equal(t, hook.LastEntry().Data[tid.TransactionIDKey], payload.(*service.EsConceptModel).PublishReference)
}

func TestIDsEndpointReturnsIDsWithInvalidIncludeTypesValue(t *testing.T) {
	ids := make(chan service.EsIDTypePair, 4)
	dummyEsService := &dummyEsService{ids: ids}

	h := NewHandler(dummyEsService, []string{"genres"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/?includeTypes=somethingDodgy", nil)

	go func() {
		ids <- service.EsIDTypePair{ID: "1", Type: "people"}
		close(ids)
	}()

	h.GetAllIds(w, req)

	for {
		line, err := w.Body.ReadString('\n')
		if err != nil {
			break
		}

		j := make(map[string]string)
		err = json.Unmarshal([]byte(line), &j)
		require.NoError(t, err)
		assert.Equal(t, "1", j["uuid"])

		_, ok := j["type"]
		assert.False(t, ok)
	}
}

func TestIDsEndpointReturnsTypes(t *testing.T) {
	ids := make(chan service.EsIDTypePair, 4)
	dummyEsService := &dummyEsService{ids: ids}

	h := NewHandler(dummyEsService, []string{"genres"})
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/?includeTypes=true", nil)

	go func() {
		ids <- service.EsIDTypePair{ID: "1", Type: "people"}
		close(ids)
	}()

	h.GetAllIds(w, req)

	for {
		line, err := w.Body.ReadString('\n')
		if err != nil {
			break
		}

		j := make(map[string]string)
		err = json.Unmarshal([]byte(line), &j)
		require.NoError(t, err)
		assert.Equal(t, "1", j["uuid"])
		assert.Equal(t, "people", j["type"])
	}
}

type dummyEsService struct {
	noop         bool
	returnsError error
	found        bool
	source       *json.RawMessage
	ids          chan service.EsIDTypePair
}

func (service *dummyEsService) LoadData(ctx context.Context, conceptType string, uuid string, payload service.EsModel) (bool, *elastic.IndexResponse, error) {
	if service.returnsError != nil {
		return false, nil, service.returnsError
	}
	if service.noop {
		return false, nil, nil
	}
	return true, &elastic.IndexResponse{}, nil
}

func (service *dummyEsService) CleanupData(ctx context.Context, concept service.Concept) {
}

func (service *dummyEsService) ReadData(conceptType string, uuid string) (*elastic.GetResult, error) {
	if service.returnsError != nil {
		return nil, service.returnsError
	}
	return &elastic.GetResult{Found: service.found, Source: service.source}, nil
}

func (service *dummyEsService) DeleteData(ctx context.Context, conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	if service.returnsError != nil {
		return nil, service.returnsError
	}
	return &elastic.DeleteResponse{Found: service.found}, nil
}

func (service *dummyEsService) LoadBulkData(conceptType string, uuid string, payload interface{}) {

}

func (service *dummyEsService) PatchUpdateConcept(ctx context.Context, conceptType string, uuid string, payload service.PayloadPatch) {

}

func (service *dummyEsService) IsIndexReadOnly() (bool, string, error) {
	return true, "", nil
}

func (service *dummyEsService) CloseBulkProcessor() error {
	if service.returnsError != nil {
		return service.returnsError
	}
	return nil
}

func (service *dummyEsService) GetClusterHealth() (*elastic.ClusterHealthResponse, error) {
	return nil, nil
}

func (service *dummyEsService) GetAllIds(ctx context.Context) chan service.EsIDTypePair {
	return service.ids
}
