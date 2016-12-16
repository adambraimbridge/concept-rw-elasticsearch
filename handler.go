package main

import (
	"encoding/json"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"net/http"
)

type conceptWriter struct {
	elasticService      *esServiceI
	allowedConceptTypes map[string]bool
}

func newESWriter(elasticService *esServiceI, allowedConceptTypes []string) (service *conceptWriter) {

	allowedTypes := make(map[string]bool)
	for _, v := range allowedConceptTypes {
		allowedTypes[v] = true
	}

	return &conceptWriter{elasticService: elasticService, allowedConceptTypes: allowedTypes}
}

func (service *conceptWriter) loadData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	if !service.allowedConceptTypes[conceptType] {
		return
	}

	var concept conceptModel
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&concept)
	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer request.Body.Close()

	if concept.UUID != uuid || concept.DirectType == "" || concept.PrefLabel == "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	payload := convertToESConceptModel(concept, conceptType)

	_, err = (*service.elasticService).loadData(conceptType, uuid, payload)
	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (service *conceptWriter) loadBulkData(writer http.ResponseWriter, request *http.Request) {
	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	if !service.allowedConceptTypes[conceptType] {
		return
	}

	var concept conceptModel
	decoder := json.NewDecoder(request.Body)
	err := decoder.Decode(&concept)
	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	defer request.Body.Close()

	if concept.UUID != uuid || concept.DirectType == "" || concept.PrefLabel == "" {
		writer.WriteHeader(http.StatusBadRequest)
		return
	}

	payload := convertToESConceptModel(concept, conceptType)
	(*service.elasticService).loadBulkData(conceptType, uuid, payload)
}

func (service *conceptWriter) readData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	getResult, err := (*service.elasticService).readData(conceptType, uuid)

	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !getResult.Found {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	writer.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(writer)
	enc.Encode(getResult.Source)

}

func (service *conceptWriter) deleteData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	res, err := (*service.elasticService).deleteData(conceptType, uuid)

	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !res.Found {
		writer.WriteHeader(http.StatusNotFound)
		return
	}
}
