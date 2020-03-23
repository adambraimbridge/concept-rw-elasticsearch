package service

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Financial-Times/go-logger"
	tid "github.com/Financial-Times/transactionid-utils-go"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	testLog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v5"
)

const (
	apiBaseURL = "http://test.api.ft.com"

	indexName         = "concept"
	organisationsType = "organisations"

	testTID          = "tid_test"
	testLastModified = "2020-03-06T13:57:57+02:00"
)

func TestNoElasticClient(t *testing.T) {
	service := esService{sync.RWMutex{}, nil, nil, "test", nil, time.Now}

	_, err := service.ReadData("any", "any")

	assert.Equal(t, ErrNoElasticClient, err, "error response")
}

func TestWriteWithGenericError(t *testing.T) {
	hook := testLog.NewLocal(logger.Logger())
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer es.Close()
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	ec := getElasticClient(t, es.URL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig, time.Now}
	testUUID := uuid.NewV4().String()
	_, up, _, err := writeTestDocument(service, organisationsType, testUUID)
	assert.EqualError(t, err, "unexpected end of JSON input")
	require.NotNil(t, hook.LastEntry())
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType, hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUUID, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "unexpected end of JSON input")
	assert.Equal(t, "unknown", hook.LastEntry().Data[statusField])
	assert.Equal(t, "write", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
	assert.False(t, up, "updated was false")
}

func TestWriteWithESError(t *testing.T) {
	hook := testLog.NewLocal(logger.Logger())
	es := newBrokenESMock()
	defer es.Close()
	bulkProcessorConfig := NewBulkProcessorConfig(1, 1, 1, time.Second)
	ec := getElasticClient(t, es.URL)
	bulkProcessor, err := newBulkProcessor(ec, &bulkProcessorConfig)
	require.NoError(t, err, "require a bulk processor")

	service := &esService{sync.RWMutex{}, ec, bulkProcessor, indexName, &bulkProcessorConfig, time.Now}
	testUUID := uuid.NewV4().String()
	_, up, _, err := writeTestDocument(service, organisationsType, testUUID)

	assert.EqualError(t, err, "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType, hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUUID, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, "500", hook.LastEntry().Data[statusField])
	assert.Equal(t, "write", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
	assert.False(t, up, "updated was false")
}

func TestDeleteWithESError(t *testing.T) {
	hook := testLog.NewLocal(logger.Logger())
	es := newBrokenESMock()
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil, time.Now}

	testUUID := uuid.NewV4().String()
	_, err = service.DeleteData(newTestContext(), organisationsType+"s", testUUID)

	assert.EqualError(t, err, "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType+"s", hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUUID, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, "500", hook.LastEntry().Data[statusField])
	assert.Equal(t, "delete", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func TestDeleteWithGenericError(t *testing.T) {
	hook := testLog.NewLocal(logger.Logger())
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil, time.Now}

	testUUID := uuid.NewV4().String()

	_, err = service.DeleteData(newTestContext(), organisationsType+"s", testUUID)

	assert.EqualError(t, err, "unexpected end of JSON input")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	assert.Equal(t, "Failed operation to Elasticsearch", hook.LastEntry().Message)
	assert.Equal(t, organisationsType+"s", hook.LastEntry().Data[conceptTypeField])
	assert.Equal(t, testUUID, hook.LastEntry().Data[uuidField])
	assert.EqualError(t, hook.LastEntry().Data["error"].(error), "unexpected end of JSON input")
	assert.Equal(t, "unknown", hook.LastEntry().Data[statusField])
	assert.Equal(t, "delete", hook.LastEntry().Data[operationField])
	assert.Equal(t, testTID, hook.LastEntry().Data[tid.TransactionIDKey])
}

func TestCleanupErrorLogging(t *testing.T) {
	hook := testLog.NewLocal(logger.Logger())
	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer es.Close()
	ec, err := elastic.NewClient(
		elastic.SetURL(es.URL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil, time.Now}

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

func newBrokenESMock() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
}

func getElasticClient(t *testing.T, url string) *elastic.Client {

	config := EsAccessConfig{
		esEndpoint:   url,
		traceLogging: false,
	}

	ec, err := NewElasticClient("local", config)
	require.NoError(t, err, "expected no error for ES client")
	return ec
}

func writeTestDocument(es EsService, conceptType string, uuid string) (EsConceptModel, bool, *elastic.IndexResponse, error) {
	payload := EsConceptModel{
		Id:           uuid,
		ApiUrl:       fmt.Sprintf("%s/%s/%s", apiBaseURL, conceptType, uuid),
		PrefLabel:    fmt.Sprintf("Test concept %s %s", conceptType, uuid),
		Types:        []string{},
		DirectType:   "",
		Aliases:      []string{},
		LastModified: testLastModified,
	}

	update, resp, err := es.LoadData(newTestContext(), conceptType, uuid, payload)
	return payload, update, resp, err
}

func newTestContext() context.Context {
	return tid.TransactionAwareContext(context.Background(), testTID)
}
