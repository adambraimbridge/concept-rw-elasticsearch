package service

import (
	"context"
	"errors"
	"sync"

	log "github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
	"strconv"
)

var (
	ErrNoElasticClient error = errors.New("No ElasticSearch client available")
)

const conceptTypeField = "conceptType"
const uuidField = "uuid"
const statusField = "status"
const oparationField = "operation"

type esService struct {
	sync.RWMutex
	elasticClient       *elastic.Client
	bulkProcessor       *elastic.BulkProcessor
	indexName           string
	bulkProcessorConfig *BulkProcessorConfig
}

type EsServiceI interface {
	LoadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error)
	ReadData(conceptType string, uuid string) (*elastic.GetResult, error)
	DeleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error)
	LoadBulkData(conceptType string, uuid string, payload interface{})
	CleanupData(conceptType string, concept Concept)
	CloseBulkProcessor() error
}

type EsHealthServiceI interface {
	GetClusterHealth() (*elastic.ClusterHealthResponse, error)
}

func NewEsService(ch chan *elastic.Client, indexName string, bulkProcessorConfig *BulkProcessorConfig) *esService {
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
		es.CloseBulkProcessor()
	}

	if es.bulkProcessorConfig != nil {
		bulkProcessor, err := newBulkProcessor(ec, es.bulkProcessorConfig)
		if err != nil {
			log.Errorf("Creating bulk processor failed with error=[%v]", err)
		}
		es.bulkProcessor = bulkProcessor
	}
}

func (es *esService) GetClusterHealth() (*elastic.ClusterHealthResponse, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	return es.elasticClient.ClusterHealth().Do(context.Background())
}

func (es *esService) LoadData(conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := es.elasticClient.Index().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do(context.Background())

	if err != nil {
		var status string
		switch err.(type) {
		case *elastic.Error:
			status = strconv.Itoa(err.(*elastic.Error).Status)
		default:
			status = "unknown"
		}
		log.WithError(err).
			WithField(conceptTypeField, conceptType).
			WithField(uuidField, uuid).
			WithField(statusField, status).
			WithField(oparationField, "write").
			Error("Failed operation to Elasticsearch")
	}

	return resp, err
}

func (es *esService) checkElasticClient() error {
	if es.elasticClient == nil {
		return ErrNoElasticClient
	}

	return nil
}

func (es *esService) ReadData(conceptType string, uuid string) (*elastic.GetResult, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := es.elasticClient.Get().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		Do(context.Background())

	if elastic.IsNotFound(err) {
		return &elastic.GetResult{Found: false}, nil
	} else {
		return resp, err
	}
}

func (es *esService) CleanupData(conceptType string, concept Concept) {
	for _, uuid := range concept.ConcordedUUIDs() {
		log.WithField("prefUUID", concept.PreferredUUID()).WithField("uuid", uuid).Info("Cleaning up concorded uuids")
		_, err := es.DeleteData(conceptType, uuid)
		if err != nil {
			log.WithField("prefUUID", concept.PreferredUUID()).WithField("uuid", uuid).Warn("Failed to delete concorded uuid.")
		}
	}
}

func (es *esService) DeleteData(conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	if err := es.checkElasticClient(); err != nil {
		return nil, err
	}

	resp, err := es.elasticClient.Delete().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		Do(context.Background())

	if err != nil {
		var status string
		switch err.(type) {
		case *elastic.Error:
			status = strconv.Itoa(err.(*elastic.Error).Status)
		default:
			status = "unknown"
		}
		log.WithError(err).
			WithField(conceptTypeField, conceptType).
			WithField(uuidField, uuid).
			WithField(statusField, status).
			WithField(oparationField, "delete").
			Error("Failed operation to Elasticsearch")
	}

	if elastic.IsNotFound(err) {
		return &elastic.DeleteResponse{Found: false}, nil
	}
	return resp, err
}

func (es *esService) LoadBulkData(conceptType string, uuid string, payload interface{}) {
	r := elastic.NewBulkIndexRequest().Index(es.indexName).Type(conceptType).Id(uuid).Doc(payload)
	es.bulkProcessor.Add(r)
}

func (es *esService) CloseBulkProcessor() error {
	return es.bulkProcessor.Close()
}
