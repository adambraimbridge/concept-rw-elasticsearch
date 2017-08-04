package service

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	tid "github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

var (
	ErrNoElasticClient error = errors.New("No ElasticSearch client available")
)

const conceptTypeField = "conceptType"
const uuidField = "uuid"
const prefUUIDField = "prefUUID"
const statusField = "status"
const operationField = "operation"
const writeOperation = "write"
const deleteOperation = "delete"
const unknownStatus = "unknown"

const tidNotFound = "not found"

type esService struct {
	sync.RWMutex
	elasticClient       *elastic.Client
	bulkProcessor       *elastic.BulkProcessor
	indexName           string
	bulkProcessorConfig *BulkProcessorConfig
}

type EsService interface {
	LoadData(ctx context.Context, conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error)
	ReadData(conceptType string, uuid string) (*elastic.GetResult, error)
	DeleteData(ctx context.Context, conceptType string, uuid string) (*elastic.DeleteResponse, error)
	LoadBulkData(conceptType string, uuid string, payload interface{})
	CleanupData(ctx context.Context, conceptType string, concept Concept)
	CloseBulkProcessor() error
	GetClusterHealth() (*elastic.ClusterHealthResponse, error)
	IsIndexReadOnly() (bool, string, error)
}

func NewEsService(ch chan *elastic.Client, indexName string, bulkProcessorConfig *BulkProcessorConfig) EsService {
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

func (es *esService) IsIndexReadOnly() (bool, string, error) {
	es.RLock()
	defer es.RUnlock()

	if err := es.checkElasticClient(); err != nil {
		return false, "", err
	}

	resp, err := es.elasticClient.IndexGetSettings(es.indexName).Do(context.Background())
	if err != nil {
		return false, "", err
	}

	for k, v := range resp {
		if strings.HasPrefix(k, es.indexName) {
			readOnly, err := es.isIndexReadOnly(v.Settings)
			return readOnly, k, err
		}
	}

	return false, "", errors.New("No index settings found")
}

func (es *esService) isIndexReadOnly(settings map[string]interface{}) (bool, error) {
	indexSettings := settings["index"].(map[string]interface{})
	if block, hasBlockSetting := indexSettings["blocks"]; hasBlockSetting {
		if writeBlocked, hasWriteBlockSetting := block.(map[string]interface{})["write"]; hasWriteBlockSetting {
			readOnly, err := strconv.ParseBool(writeBlocked.(string))
			return readOnly, err
		}
	}

	return false, nil
}

func (es *esService) LoadData(ctx context.Context, conceptType string, uuid string, payload interface{}) (*elastic.IndexResponse, error) {
	loadDataLog := log.WithField(conceptTypeField, conceptType).
		WithField(uuidField, uuid).
		WithField(operationField, writeOperation)

	transactionID, err := tid.GetTransactionIDFromContext(ctx)
	if err != nil {
		transactionID = tidNotFound
	}
	loadDataLog = loadDataLog.WithField(tid.TransactionIDKey, transactionID)

	if err := es.checkElasticClient(); err != nil {
		loadDataLog.WithError(err).WithField(statusField, unknownStatus).Error("Failed operation to Elasticsearch")
		return nil, err
	}

	resp, err := es.elasticClient.Index().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do(ctx)

	if err != nil {
		var status string
		switch err.(type) {
		case *elastic.Error:
			status = strconv.Itoa(err.(*elastic.Error).Status)
		default:
			status = unknownStatus
		}

		loadDataLog.WithError(err).WithField(statusField, status).Error("Failed operation to Elasticsearch")
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

func (es *esService) CleanupData(ctx context.Context, conceptType string, concept Concept) {
	cleanupDataLog := log.WithField(prefUUIDField, concept.PreferredUUID()).WithField(conceptTypeField, conceptType)
	transactionID, err := tid.GetTransactionIDFromContext(ctx)
	if err != nil {
		transactionID = tidNotFound
	}
	cleanupDataLog = cleanupDataLog.WithField(tid.TransactionIDKey, transactionID)

	for _, uuid := range concept.ConcordedUUIDs() {
		cleanupDataLog.WithField(uuidField, uuid).Info("Cleaning up concorded uuids")
		_, err := es.DeleteData(ctx, conceptType, uuid)
		if err != nil {
			cleanupDataLog.WithError(err).WithField(uuidField, uuid).Error("Failed to delete concorded uuid.")
		}
	}
}

func (es *esService) DeleteData(ctx context.Context, conceptType string, uuid string) (*elastic.DeleteResponse, error) {
	deleteDataLog := log.WithField(conceptTypeField, conceptType).
		WithField(uuidField, uuid).
		WithField(operationField, deleteOperation)

	transactionID, err := tid.GetTransactionIDFromContext(ctx)
	if err != nil {
		transactionID = tidNotFound
	}
	deleteDataLog = deleteDataLog.WithField(tid.TransactionIDKey, transactionID)

	if err := es.checkElasticClient(); err != nil {
		deleteDataLog.WithError(err).
			WithField(statusField, unknownStatus).
			Error("Failed operation to Elasticsearch")
		return nil, err
	}

	resp, err := es.elasticClient.Delete().
		Index(es.indexName).
		Type(conceptType).
		Id(uuid).
		Do(ctx)

	if elastic.IsNotFound(err) {
		return &elastic.DeleteResponse{Found: false}, nil
	}

	if err != nil {
		var status string
		switch err.(type) {
		case *elastic.Error:
			status = strconv.Itoa(err.(*elastic.Error).Status)
		default:
			status = unknownStatus
		}
		deleteDataLog.WithError(err).
			WithField(statusField, status).
			Error("Failed operation to Elasticsearch")
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
