package main

import (
	"encoding/json"
	"fmt"
	"github.com/Financial-Times/neo-model-utils-go/mapper"
	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/gorilla/mux"
	"gopkg.in/olivere/elastic.v2"
	"net/http"
	"time"
)

type esWriterService struct {
	elasticClient *elastic.Client
	indexName     string
	bulkProcessor *elastic.BulkProcessor
}

type amazonAccessConfig struct {
	accessKey  string
	secretKey  string
	esEndpoint string
	esRegion   string
}

type bulkProcessorConfig struct {
	nrWorkers     int
	nrOfRequests  int
	bulkSize      int
	flushInterval time.Duration
}

func NewESWriterService(accessConfig *amazonAccessConfig, bulkConfig *bulkProcessorConfig) (service *esWriterService, err error) {
	elasticClient, err := newElasticClient(credentials.NewStaticCredentials(accessConfig.accessKey, accessConfig.secretKey, ""), &accessConfig.esEndpoint, &accessConfig.esRegion)
	if err != nil {
		return nil, fmt.Errorf("Creating elasticsearch client failed with error=[%v]\n", err)
	}

	bulkProcessor, err := elasticClient.BulkProcessor().Name("BackgroundWorker-1").
		Workers(bulkConfig.nrWorkers).
		BulkActions(bulkConfig.nrOfRequests).
		BulkSize(bulkConfig.bulkSize).
		FlushInterval(bulkConfig.flushInterval * time.Second).
		After(handleBulkFailures).
		Do()

	defer bulkProcessor.Close()

	if err != nil {
		return nil, fmt.Errorf("Creating bulk processor failed with error=[%v]\n", err)
	}

	elasticWriter := esWriterService{elasticClient: elasticClient, indexName: "concept", bulkProcessor: bulkProcessor}

	return &elasticWriter, nil
}

func (service *esWriterService) loadData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	var concept conceptModel
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&concept)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	defer request.Body.Close()

	if concept.UUID != uuid {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	payload := convertToESConceptModel(concept, conceptType)

	_, err = service.elasticClient.Index().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		BodyJson(payload).
		Do()

	if err != nil {
		log.Errorf(err.Error())
	}
}

func (service *esWriterService) loadBulkData(writer http.ResponseWriter, request *http.Request) {
	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	var concept conceptModel
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&concept)
	if err != nil {
		log.Errorf(err.Error())
		return
	}

	defer request.Body.Close()

	if concept.UUID != uuid {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	payload := convertToESConceptModel(concept, conceptType)

	r := elastic.NewBulkIndexRequest().Index(service.indexName).Type(conceptType).Id(uuid).Doc(payload)
	service.bulkProcessor.Add(r)

	if err != nil {
		log.Errorf(err.Error())
	}
}

func handleBulkFailures(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if err != nil {
		// Something went badly wrong, ES reported HTTP status outside [200,300), even after retrying
		log.Errorf("Bulk request failed with error: %v, for the following requests: %v", err, requests)
		return // response is probably nil
	}

	for _, failedItem := range response.Failed() {
		log.Errorf("Concept with uuid %s failed with the following details: %v", failedItem.Id, failedItem)
	}
}

func (service *esWriterService) readData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	getResult, err := service.elasticClient.Get().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		Do()

	if err != nil {
		log.Errorf(err.Error())
	}

	if !getResult.Found {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(writer)
	enc.Encode(getResult.Source)

}

func (service *esWriterService) deleteData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	res, err := service.elasticClient.Delete().
		Index(service.indexName).
		Type(conceptType).
		Id(uuid).
		Do()

	if err != nil {
		log.Errorf(err.Error())
	}

	if !res.Found {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
}

func convertToESConceptModel(concept conceptModel, conceptType string) esConceptModel {

	esModel := esConceptModel{}
	esModel.ApiUrl = mapper.APIURL(concept.UUID, []string{concept.DirectType}, "")
	esModel.Id = mapper.IDURL(concept.UUID)
	esModel.Types = mapper.TypeURIs(concept.Types)
	esModel.DirectType = concept.DirectType
	esModel.Aliases = concept.Aliases
	esModel.PrefLabel = concept.PrefLabel

	return esModel
}
