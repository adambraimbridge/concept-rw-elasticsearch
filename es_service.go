package main

import (
	"errors"
	"sync"

	log "github.com/Sirupsen/logrus"
	"gopkg.in/olivere/elastic.v3"
)

var (
	ErrNoElasticClient error = errors.New("No ElasticSearch client available")
)

type esService struct {
	sync.RWMutex
	elasticClient       *elastic.Client
	bulkProcessor       *elastic.BulkProcessor
	indexName           string
	bulkProcessorConfig *bulkProcessorConfig
}

type esServiceI interface {
	loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error)
	readData(conceptType string, uuid string) (*elastic.GetResult, error)
	deleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error)
	loadBulkData(conceptType string, uuid string, payload interface{})
	closeBulkProcessor() error
}

type esHealthServiceI interface {
	getClusterHealth() (*elastic.ClusterHealthResponse, error)
}

func newEsService(ch chan *elastic.Client, indexName string, bulkProcessorConfig *bulkProcessorConfig) *esService {
	es := &esService{bulkProcessorConfig: bulkProcessorConfig, indexName: indexName}
	go func() {
		for ec := range ch {
			es.setElasticClient(ec)
		}
	}()
	return es
}

func (es *esService) setElasticClient(ec *elastic.Client) {
	es.Lock()
	defer es.Unlock()

	es.elasticClient = ec

	if es.bulkProcessor != nil {
		es.closeBulkProcessor()
	}

	if es.bulkProcessorConfig != nil {
		bulkProcessor, err := newBulkProcessor(ec, es.bulkProcessorConfig)
		if err != nil {
			log.Errorf("Creating bulk processor failed with error=[%v]", err)
		}
		es.bulkProcessor = bulkProcessor
	}
}

func (es *esService) getClusterHealth() (*elastic.ClusterHealthResponse, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	return es.elasticClient.ClusterHealth().Do()
}

func (es *esService) loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	return es.elasticClient.Index().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do()
}

func (es *esService) checkElasticClient() error {
	if es.elasticClient == nil {
		return ErrNoElasticClient
	}

	return nil
}

func (es *esService) readData(conceptType string, uuid string) (*elastic.GetResult, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := es.elasticClient.Get().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		IgnoreErrorsOnGeneratedFields(false).
		Do()

	if elastic.IsNotFound(err) {
		return &elastic.GetResult{Found: false}, nil
	} else {
		return resp, err
	}
}

func (es *esService) deleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := es.elasticClient.Delete().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		Do()

	if elastic.IsNotFound(err) {
		return &elastic.DeleteResponse{Found: false}, nil
	} else {
		return resp, err
	}
}

func (es *esService) loadBulkData(conceptType string, uuid string, payload interface{}) {
	r := elastic.NewBulkIndexRequest().Index(es.indexName).Type(conceptType).Id(uuid).Doc(payload)
	es.bulkProcessor.Add(r)
}

func (es *esService) closeBulkProcessor() error {
	return es.bulkProcessor.Close()
}
