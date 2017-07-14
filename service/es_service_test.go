package service

import (
	"encoding/json"
	"fmt"
	"github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
	testLog "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v5"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	apiBaseUrl  = "http://test.api.ft.com"
	indexName   = "concept"
	conceptType = "organisation"
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

func writeDocument(es *esService, t string, u string) (EsConceptModel, *elastic.IndexResponse, error) {
	payload := EsConceptModel{
		Id:         u,
		ApiUrl:     fmt.Sprintf("%s/%ss/%s", apiBaseUrl, t, u),
		PrefLabel:  fmt.Sprintf("Test concept %s %s", t, u),
		Types:      []string{},
		DirectType: "",
		Aliases:    []string{},
	}

	resp, err := es.LoadData(t+"s", u, payload)
	return payload, resp, err
}

func TestWrite(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	_, resp, err := writeDocument(service, conceptType, testUuid)
	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, true, resp.Created, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, conceptType+"s", resp.Type, "concept type")
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
	_, _, err = writeDocument(service, conceptType, testUuid)
	assert.EqualError(t, err, "unexpected end of JSON input")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	expectedMsg := fmt.Sprintf("Concept organisations with uuid %v failed with status code unknown and the following details: unexpected end of JSON input", testUuid)
	assert.Equal(t, expectedMsg, hook.LastEntry().Message)
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
	_, _, err = writeDocument(service, conceptType, testUuid)
	assert.EqualError(t, err, "elastic: Error 500 (Internal Server Error)")
	assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	expectedMsg := fmt.Sprintf("Concept organisations with uuid %v failed with status code 500 and the following details: elastic: Error 500 (Internal Server Error)", testUuid)
	assert.Equal(t, expectedMsg, hook.LastEntry().Message)
}

func newBrokenESMock() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
}

func TestRead(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	payload, _, err := writeDocument(service, conceptType, testUuid)
	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(conceptType+"s", testUuid)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func TestPassClientThroughChannel(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ecc := make(chan *elastic.Client)
	defer close(ecc)

	service := NewEsService(ecc, indexName, nil)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	ecc <- ec

	waitForClientInjection(service)

	testUuid := uuid.NewV4().String()
	payload, _, err := writeDocument(service, conceptType, testUuid)
	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(conceptType+"s", testUuid)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)

	assert.Equal(t, fmt.Sprintf("%s/%ss/%s", apiBaseUrl, conceptType, testUuid), obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func waitForClientInjection(service *esService) {
	for i := 0; i < 10; i++ {
		_, err := service.GetClusterHealth()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
}
