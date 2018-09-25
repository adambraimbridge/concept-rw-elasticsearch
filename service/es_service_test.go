package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	tid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	testLog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v5"
)

const (
	apiBaseUrl        = "http://test.api.ft.com"
	indexName         = "concept"
	organisationsType = "organisations"
	peopleType        = "people"
	testTID           = "tid_test"

	esStatusCreated = "created"
)

func TestNoElasticClient(t *testing.T) {
	service := esService{sync.RWMutex{}, nil, nil, "test", nil}

	_, err := service.ReadData("any", "any")

	assert.Equal(t, ErrNoElasticClient, err, "error response")
}

func getElasticSearchTestURL(t *testing.T) string {
	if testing.Short() {
		t.Skip("ElasticSearch integration for long tests only.")
	}

	esURL := os.Getenv("ELASTICSEARCH_TEST_URL")
	if strings.TrimSpace(esURL) == "" {
		t.Fatal("Please set the environment variable ELASTICSEARCH_TEST_URL to run ElasticSearch integration tests (e.g. export ELASTICSEARCH_TEST_URL=http://localhost:9200). Alternatively, run `go test -short` to skip them.")
	}

	return esURL
}

func getElasticClient(t *testing.T, url string) *elastic.Client {
	ec, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	return ec
}

func setReadOnly(t *testing.T, client *elastic.Client, indexName string, readOnly bool) {
	indexService := elastic.NewIndicesPutSettingsService(client)

	_, err := indexService.Index(indexName).BodyJson(map[string]interface{}{"index.blocks.write": strconv.FormatBool(readOnly)}).Do(context.Background())

	assert.NoError(t, err, "expected no error for putting index settings")
}

func writeDocument(es EsService, t string, u string) (EsConceptModel, *elastic.IndexResponse, error) {
	payload := EsConceptModel{
		Id:         u,
		ApiUrl:     fmt.Sprintf("%s/%ss/%s", apiBaseUrl, t, u),
		PrefLabel:  fmt.Sprintf("Test concept %s %s", t, u),
		Types:      []string{},
		DirectType: "",
		Aliases:    []string{},
	}

	resp, err := es.LoadData(newTestContext(), t, u, payload)
	return payload, resp, err
}

func newTestContext() context.Context {
	return tid.TransactionAwareContext(context.Background(), testTID)
}

func TestWrite(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	_, resp, err := writeDocument(service, organisationsType, testUuid)
	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUuid, resp.Id, "document id")
}

func TestWriteWithGenericError(t *testing.T) {
	hook := testLog.NewGlobal()
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	_, _, err = writeDocument(service, organisationsType, testUuid)
	assert.EqualError(t, err, "unexpected end of JSON input")
	require.NotNil(t, hook.LastEntry())

	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType, hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUuid, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "unexpected end of JSON input")
	assert.Equal(t, "unknown", hook.LastEntry().Data[statusField])
	assert.Equal(t, "write", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func TestWriteWithESError(t *testing.T) {
	hook := testLog.NewGlobal()
	es := newBrokenESMock()
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	_, _, err = writeDocument(service, organisationsType, testUuid)
	assert.EqualError(t, err, "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType, hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUuid, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, "500", hook.LastEntry().Data[statusField])
	assert.Equal(t, "write", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func newBrokenESMock() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
}

func TestIsReadOnly(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	readOnly, name, err := service.IsIndexReadOnly()
	assert.False(t, readOnly, "index should not be read-only")
	assert.Equal(t, name, indexName, "index name should be returned")
	assert.NoError(t, err, "read-only check should not return an error")

	setReadOnly(t, ec, indexName, true)
	defer setReadOnly(t, ec, indexName, false)

	readOnly, name, err = service.IsIndexReadOnly()
	assert.True(t, readOnly, "index should be read-only")
	assert.Equal(t, name, indexName, "index name should be returned")
	assert.NoError(t, err, "read-only check should not return an error")
}

func TestIsReadOnlyIndexNotFound(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, "foo", nil}

	readOnly, name, err := service.IsIndexReadOnly()
	assert.False(t, readOnly, "index should not be read-only")
	assert.Empty(t, name, "no index name should be returned")
	assert.Error(t, err, "index should not be found")
}

func TestRead(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	payload, _, err := writeDocument(service, organisationsType, testUuid)
	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(organisationsType, testUuid)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func TestDeleteWithESError(t *testing.T) {
	hook := testLog.NewGlobal()
	es := newBrokenESMock()
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	_, err = service.DeleteData(newTestContext(), organisationsType+"s", testUuid)

	assert.EqualError(t, err, "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType+"s", hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUuid, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, "500", hook.LastEntry().Data[statusField])
	assert.Equal(t, "delete", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func TestPassClientThroughChannel(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ecc := make(chan *elastic.Client)
	defer close(ecc)

	service := NewEsService(ecc, indexName, nil)

	ec := getElasticClient(t, esURL)

	ecc <- ec

	waitForClientInjection(service)

	testUuid := uuid.NewV4().String()
	payload, _, err := writeDocument(service, organisationsType, testUuid)
	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(organisationsType, testUuid)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)

	assert.Equal(t, fmt.Sprintf("%s/%ss/%s", apiBaseUrl, organisationsType, testUuid), obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func TestDelete(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID := uuid.NewV4().String()
	_, resp, err := writeDocument(service, organisationsType, testUUID)
	require.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	deleteResp, err := service.DeleteData(newTestContext(), organisationsType, testUUID)
	require.NoError(t, err)
	assert.True(t, deleteResp.Found)

	getResp, err := service.ReadData(organisationsType, testUUID)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)
}

func TestDeleteNotFoundConcept(t *testing.T) {
	hook := testLog.NewGlobal()
	esURL := getElasticSearchTestURL(t)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	resp, err := service.DeleteData(newTestContext(), organisationsType+"s", testUuid)

	assert.False(t, resp.Found, "document is not found")

	assert.Empty(t, hook.AllEntries(), "It logged nothing")
}

func TestDeleteWithGenericError(t *testing.T) {
	hook := testLog.NewGlobal()
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()

	_, err = service.DeleteData(newTestContext(), organisationsType+"s", testUuid)

	assert.EqualError(t, err, "unexpected end of JSON input")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType+"s", hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUuid, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "unexpected end of JSON input")
	assert.Equal(t, "unknown", hook.LastEntry().Data[statusField])
	assert.Equal(t, "delete", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func TestCleanup(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID1 := uuid.NewV4().String()
	_, resp, err := writeDocument(service, organisationsType, testUUID1)
	require.NoError(t, err, "expected successful write")
	require.Equal(t, esStatusCreated, resp.Result, "document should have been created")

	testUUID2 := uuid.NewV4().String()
	_, resp, err = writeDocument(service, peopleType, testUUID2)
	require.NoError(t, err, "expected successful write")
	require.Equal(t, esStatusCreated, resp.Result, "document should have been created")

	testUUID3 := uuid.NewV4().String()
	_, resp, err = writeDocument(service, organisationsType, testUUID3)
	require.NoError(t, err, "expected successful write")
	require.Equal(t, esStatusCreated, resp.Result, "document should have been created")

	concept := AggregateConceptModel{PrefUUID: testUUID1, SourceRepresentations: []SourceConcept{
		{
			UUID: testUUID1,
		},
		{
			UUID: testUUID2,
		},
		{
			UUID: testUUID3,
		},
	}}

	// ensure test data is immediately available from the index
	_, err = ec.Refresh(indexName).Do(context.Background())
	require.NoError(t, err)

	service.CleanupData(newTestContext(), concept)

	getResp, err := service.ReadData(peopleType, testUUID2)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)

	getResp, err = service.ReadData(organisationsType, testUUID3)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)

	getResp, err = service.ReadData(organisationsType, testUUID1)
	assert.NoError(t, err)
	assert.True(t, getResp.Found)
}

func TestCleanupErrorLogging(t *testing.T) {
	hook := testLog.NewGlobal()
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID1 := uuid.NewV4().String()
	testUUID2 := uuid.NewV4().String()

	concept := AggregateConceptModel{PrefUUID: testUUID2, SourceRepresentations: []SourceConcept{
		{
			UUID: testUUID1,
		},
		{
			UUID: testUUID2,
		},
	}}

	service.CleanupData(newTestContext(), concept)

	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Impossible to find concorded concepts in elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, testUUID2, hook.LastEntry().Data[prefUUIDField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "unexpected end of JSON input")
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func TestDeprecationFlagTrue(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID := uuid.NewV4().String()
	payload := EsConceptModel{
		Id:           testUUID,
		ApiUrl:       fmt.Sprintf("%s/%ss/%s", apiBaseUrl, organisationsType, testUUID),
		PrefLabel:    fmt.Sprintf("Test concept %s %s", organisationsType, testUUID),
		Types:        []string{},
		DirectType:   "",
		Aliases:      []string{},
		IsDeprecated: true,
	}

	resp, err := service.LoadData(newTestContext(), organisationsType, testUUID, payload)
	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	readResp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, readResp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*readResp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
	assert.Equal(t, true, obj["isDeprecated"], "deprecation flag")
}

func TestDeprecationFlagFalse(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID := uuid.NewV4().String()
	payload := EsConceptModel{
		Id:         testUUID,
		ApiUrl:     fmt.Sprintf("%s/%ss/%s", apiBaseUrl, organisationsType, testUUID),
		PrefLabel:  fmt.Sprintf("Test concept %s %s", organisationsType, testUUID),
		Types:      []string{},
		DirectType: "",
		Aliases:    []string{},
	}

	resp, err := service.LoadData(newTestContext(), organisationsType, testUUID, payload)
	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	readResp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, readResp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*readResp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
	_, deprecatedFlagExists := obj["isDeprecated"]
	assert.False(t, deprecatedFlagExists, "deprecation flag")
}

func TestMetricsUpdated(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)

	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	ch := make(chan *elastic.Client)

	service := NewEsService(ch, indexName, &bulkProcessorConfig)

	ch <- ec // will block until es service has received it

	testUUID := uuid.NewV4().String()
	payload := EsConceptModel{
		Id:         testUUID,
		ApiUrl:     fmt.Sprintf("%s/%ss/%s", apiBaseUrl, organisationsType, testUUID),
		PrefLabel:  fmt.Sprintf("Test concept %s %s", organisationsType, testUUID),
		Types:      []string{},
		DirectType: "",
		Aliases:    []string{},
	}

	resp, err := service.LoadData(newTestContext(), organisationsType, testUUID, payload)
	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, esStatusCreated, resp.Result, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, organisationsType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	testMetrics := &MetricsPayload{Metrics: &ConceptMetrics{AnnotationsCount: 150000}}
	service.PatchUpdateDataWithMetrics(newTestContext(), organisationsType, testUUID, testMetrics)

	service.(*esService).bulkProcessor.Flush() // wait for the bulk processor to write the data

	readResp, err := service.ReadData(organisationsType, testUUID)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, readResp.Found, "should find a result")

	actualModel := EsConceptModel{}
	err = json.Unmarshal(*readResp.Source, &actualModel)

	assert.NoError(t, err)

	assert.Equal(t, payload.ApiUrl, actualModel.ApiUrl, "Expect the original fields to still be intact")
	assert.Equal(t, payload.PrefLabel, actualModel.PrefLabel, "Expect the original fields to still be intact")

	assert.Equal(t, testMetrics.Metrics.AnnotationsCount, actualModel.Metrics.AnnotationsCount, "Count should be set")
}

func waitForClientInjection(service EsService) {
	for i := 0; i < 10; i++ {
		_, err := service.GetClusterHealth()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestGetAllIds(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	expected := make([]string, 0)
	for i := 0; i < 1001; i++ {
		testUuid := uuid.NewV4().String()
		_, _, err := writeDocument(service, organisationsType, testUuid)
		require.NoError(t, err, "expected successful write")
		expected = append(expected, testUuid)
	}
	_, err := ec.Refresh(indexName).Do(context.Background())
	require.NoError(t, err, "expected successful flush")

	ch := service.GetAllIds(context.Background())
	actual := make(map[string]struct{})
	for id := range ch {
		actual[id] = struct{}{}
	}

	notFound := 0
	for _, id := range expected {
		_, found := actual[id]
		if !found {
			notFound++
		}
	}
	assert.Equal(t, 0, notFound, "UUIDs not found")
}
