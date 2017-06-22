package resources

import (
	"encoding/json"
	"net/http"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

type Handler struct {
	elasticService      service.EsServiceI
	modelPopulater      service.ModelPopulater
	allowedConceptTypes map[string]bool
}

func NewHandler(elasticService service.EsServiceI, authorService service.AuthorService, allowedConceptTypes []string) *Handler {

	allowedTypes := make(map[string]bool)
	for _, v := range allowedConceptTypes {
		allowedTypes[v] = true
	}

	esModelPopulater := service.NewEsModelPopulater(authorService)

	return &Handler{elasticService: elasticService, modelPopulater: esModelPopulater, allowedConceptTypes: allowedTypes}
}

func (h *Handler) LoadData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	if !h.allowedConceptTypes[conceptType] {
		return
	}

	var concept service.ConceptModel
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

	payload := h.modelPopulater.ConvertToESConceptModel(concept, conceptType)

	_, err = h.elasticService.LoadData(conceptType, uuid, payload)
	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

func (h *Handler) LoadBulkData(writer http.ResponseWriter, request *http.Request) {
	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	if !h.allowedConceptTypes[conceptType] {
		return
	}

	var concept service.ConceptModel
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

	payload := h.modelPopulater.ConvertToESConceptModel(concept, conceptType)
	h.elasticService.LoadBulkData(conceptType, uuid, payload)
	writer.WriteHeader(http.StatusOK)
}

func (h *Handler) ReadData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	getResult, err := h.elasticService.ReadData(conceptType, uuid)

	if err != nil {
		log.Errorf(err.Error())

		if err == service.ErrNoElasticClient {
			writer.WriteHeader(http.StatusServiceUnavailable)
		} else {
			writer.WriteHeader(http.StatusInternalServerError)
		}

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

func (h *Handler) DeleteData(writer http.ResponseWriter, request *http.Request) {

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	res, err := h.elasticService.DeleteData(conceptType, uuid)

	if err != nil {
		log.Errorf(err.Error())
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	if !res.Found {
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	writer.WriteHeader(http.StatusOK)
}

func (h *Handler) Close() {
	h.elasticService.CloseBulkProcessor()
}
