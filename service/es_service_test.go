package service

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/olivere/elastic.v5"
)

const (
	apiBaseUrl  = "http://test.api.ft.com"
	indexName   = "concept"
	conceptType = "organisations"
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

func writeDocument(es *esService, t string, u string) (EsConceptModel, *elastic.IndexResponse, error) {
	payload := EsConceptModel{
		Id:         u,
		ApiUrl:     fmt.Sprintf("%s/%ss/%s", apiBaseUrl, t, u),
		PrefLabel:  fmt.Sprintf("Test concept %s %s", t, u),
		Types:      []string{},
		DirectType: "",
		Aliases:    []string{},
	}

	resp, err := es.LoadData(t, u, payload)
	return payload, resp, err
}

func TestWrite(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUuid := uuid.NewV4().String()
	_, resp, err := writeDocument(service, conceptType, testUuid)
	assert.NoError(t, err, "expected successful write")

	assert.Equal(t, true, resp.Created, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, conceptType, resp.Type, "concept type")
	assert.Equal(t, testUuid, resp.Id, "document id")
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
	payload, _, err := writeDocument(service, conceptType, testUuid)
	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(conceptType, testUuid)

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

	ec := getElasticClient(t, esURL)

	ecc <- ec

	waitForClientInjection(service)

	testUuid := uuid.NewV4().String()
	payload, _, err := writeDocument(service, conceptType, testUuid)
	assert.NoError(t, err, "expected successful write")

	resp, err := service.ReadData(conceptType, testUuid)

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)

	assert.Equal(t, fmt.Sprintf("%s/%ss/%s", apiBaseUrl, conceptType, testUuid), obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.ApiUrl, obj["apiUrl"], "apiUrl")
	assert.Equal(t, payload.PrefLabel, obj["prefLabel"], "prefLabel")
}

func TestDelete(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID := uuid.NewV4().String()
	_, resp, err := writeDocument(service, conceptType, testUUID)
	require.NoError(t, err, "expected successful write")

	assert.True(t, resp.Created, "document should have been created")
	assert.Equal(t, indexName, resp.Index, "index name")
	assert.Equal(t, conceptType, resp.Type, "concept type")
	assert.Equal(t, testUUID, resp.Id, "document id")

	deleteResp, err := service.DeleteData(conceptType, testUUID)
	require.NoError(t, err)
	assert.True(t, deleteResp.Found)

	getResp, err := service.ReadData(conceptType, testUUID)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)
}

func TestCleanup(t *testing.T) {
	esURL := getElasticSearchTestURL(t)
	ec := getElasticClient(t, esURL)
	service := &esService{sync.RWMutex{}, ec, nil, indexName, nil}

	testUUID1 := uuid.NewV4().String()
	_, resp, err := writeDocument(service, conceptType, testUUID1)
	require.NoError(t, err, "expected successful write")
	require.True(t, resp.Created, "document should have been created")

	testUUID2 := uuid.NewV4().String()
	_, resp, err = writeDocument(service, conceptType, testUUID2)
	require.NoError(t, err, "expected successful write")
	require.True(t, resp.Created, "document should have been created")

	concept := AggregateConceptModel{PrefUUID: testUUID2, SourceRepresentations: []SourceConcept{
		{
			UUID: testUUID1,
		},
		{
			UUID: testUUID2,
		},
	}}

	ct := conceptType
	service.CleanupData(ct, concept)

	getResp, err := service.ReadData(ct, testUUID1)
	assert.NoError(t, err)
	assert.False(t, getResp.Found)

	getResp, err = service.ReadData(ct, testUUID2)
	assert.NoError(t, err)
	assert.True(t, getResp.Found)
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
