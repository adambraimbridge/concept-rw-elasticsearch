package main

import (
	"gopkg.in/olivere/elastic.v3"
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

func newEsService(client *elastic.Client, indexName string, bulkProcessor *elastic.BulkProcessor) *esService {
	return &esService{elasticClient: client, bulkProcessor: bulkProcessor, indexName: indexName}
}

func (service esService) loadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	return service.elasticClient.Index().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do()
}

func (service esService) readData(conceptType string, uuid string) (*elastic.GetResult, error) {
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

func (service esService) deleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error) {
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

func (service esService) loadBulkData(conceptType string, uuid string, payload interface{}) {
	r := elastic.NewBulkIndexRequest().Index(service.indexName).Type(conceptType).Id(uuid).Doc(payload)
	service.bulkProcessor.Add(r)
}

func (service esService) closeBulkProcessor() error {
	return service.bulkProcessor.Close()
}
