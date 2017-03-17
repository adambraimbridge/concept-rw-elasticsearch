package main

import (
	"errors"

	"gopkg.in/olivere/elastic.v3"
)

var (
	ErrNoElasticClient error = errors.New("No ElasticSearch client available")
)

type esService struct {
	elasticClient *elastic.Client
	bulkProcessor *elastic.BulkProcessor
	indexName     string
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

func newEsService(client *elastic.Client, indexName string, bulkProcessor *elastic.BulkProcessor) *esService {
	return &esService{elasticClient: client, bulkProcessor: bulkProcessor, indexName: indexName}
}

func (esService *esService) getClusterHealth() (*elastic.ClusterHealthResponse, error) {
	if err := esService.checkElasticClient(); err != nil {
		return nil, err
	}

	return esService.elasticClient.ClusterHealth().Do()
}

func (service *esService) loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	if err := service.checkElasticClient(); err != nil {
		return nil, err
	}

	return service.elasticClient.Index().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do()
}

func (service *esService) checkElasticClient() error {
	if service.elasticClient == nil {
		return ErrNoElasticClient
	}

	return nil
}

func (service *esService) readData(conceptType string, uuid string) (*elastic.GetResult, error) {
	if err := service.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := service.elasticClient.Get().
		Index(service.indexName).
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

func (service *esService) deleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	if err := service.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := service.elasticClient.Delete().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		Do()

	if elastic.IsNotFound(err) {
		return &elastic.DeleteResponse{Found: false}, nil
	} else {
		return resp, err
	}
}

func (service *esService) loadBulkData(conceptType string, uuid string, payload interface{}) {
	r := elastic.NewBulkIndexRequest().Index(service.indexName).Type(conceptType).Id(uuid).Doc(payload)
	service.bulkProcessor.Add(r)
}

func (service *esService) closeBulkProcessor() error {
	return service.bulkProcessor.Close()
}
