package main

import (
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/olivere/elastic.v3"
)

func TestNoElasticClient(t *testing.T) {
	service := esService{sync.RWMutex{}, nil, nil, "test", nil}

	_, err := service.readData("any", "any")

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

func TestRead(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	service := esService{sync.RWMutex{}, ec, nil, "concept", nil}

	resp, err := service.readData("organisations", "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8")

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)
	assert.Equal(t, "http://api.ft.com/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8", obj["apiUrl"], "apiUrl")
}

func TestPassClientThroughChannel(t *testing.T) {
	esURL := getElasticSearchTestURL(t)

	ecc := make(chan *elastic.Client)
	defer close(ecc)

	service := newEsService(ecc, "concept", nil)

	ec, err := elastic.NewClient(
		elastic.SetURL(esURL),
		elastic.SetSniff(false),
	)
	assert.NoError(t, err, "expected no error for ES client")

	ecc <- ec

	waitForClientInjection(service)

	resp, err := service.readData("organisations", "2384fa7a-d514-3d6a-a0ea-3a711f66d0d8")

	assert.NoError(t, err, "expected no error for ES read")
	assert.True(t, resp.Found, "should find a result")

	obj := make(map[string]interface{})
	err = json.Unmarshal(*resp.Source, &obj)

	assert.Equal(t, "http://api.ft.com/organisations/2384fa7a-d514-3d6a-a0ea-3a711f66d0d8", obj["apiUrl"], "apiUrl")
}

func waitForClientInjection(service *esService) {
	for i := 0; i < 10; i++ {
		_, err := service.getClusterHealth()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

}
