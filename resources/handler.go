package resources

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"context"
	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	tid "github.com/Financial-Times/transactionid-utils-go"
	"github.com/gorilla/mux"
	log "github.com/sirupsen/logrus"
)

var (
	errPathUUID               = errors.New("Provided path UUID does not match request body")
	errInvalidConceptModel    = errors.New("Invalid or incomplete concept model")
	errUnsupportedConceptType = errors.New("Unsupported or invalid concept type")
	errProcessingBody         = errors.New("Request body is not in the expected concept model format")
)

// Handler handles http calls
type Handler struct {
	elasticService      service.EsService
	modelPopulator      service.ModelPopulator
	allowedConceptTypes map[string]bool
}

func NewHandler(elasticService service.EsService, authorService service.AuthorService, allowedConceptTypes []string) *Handler {
	allowedTypes := make(map[string]bool)
	for _, v := range allowedConceptTypes {
		allowedTypes[v] = true
	}

	esModelPopulator := service.NewEsModelPopulator(authorService)

	return &Handler{elasticService: elasticService, modelPopulator: esModelPopulator, allowedConceptTypes: allowedTypes}
}

// LoadData processes a single ES concept entity
func (h *Handler) LoadData(w http.ResponseWriter, r *http.Request) {
	transactionID := tid.GetTransactionIDFromRequest(r)
	ctx := tid.TransactionAwareContext(context.Background(), transactionID)

	conceptType, concept, payload, err := h.processPayload(r)
	if err != nil {
		writeMessage(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = h.elasticService.LoadData(conceptType, concept.PreferredUUID(), *payload, ctx)
	if err == service.ErrNoElasticClient {
		writeMessage(w, "ES unavailable", http.StatusServiceUnavailable)
		return
	}

	if err != nil {
		log.WithError(err).Warn("Failed to write data to elasticsearch.")
		writeMessage(w, "Failed to write data to ES", http.StatusInternalServerError)
		return
	}

	h.elasticService.CleanupData(conceptType, concept, ctx)

	writeMessage(w, "Concept written successfully", http.StatusOK)
}

// LoadBulkData write a concept to ES via the ES Bulk API
func (h *Handler) LoadBulkData(w http.ResponseWriter, r *http.Request) {
	conceptType, concept, payload, err := h.processPayload(r)
	if err != nil {
		writeMessage(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.elasticService.LoadBulkData(conceptType, concept.PreferredUUID(), *payload)
	h.elasticService.CleanupData(conceptType, concept, context.Background())
	writeMessage(w, "Concept written successfully", http.StatusOK)
}

func (h *Handler) processPayload(r *http.Request) (string, service.Concept, *interface{}, error) {
	vars := mux.Vars(r)
	uuid := vars["id"]
	conceptType := vars["concept-type"]

	if !h.allowedConceptTypes[conceptType] {
		return "", nil, nil, errUnsupportedConceptType
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("Failed to read request body")
		return "", nil, nil, errProcessingBody
	}

	aggConceptModel, err := isAggregateConceptModel(body)
	if err != nil {
		log.WithError(err).Error("Failed to check if body json is an aggregate concept model or not")
		return "", nil, nil, errProcessingBody
	}

	var concept service.Concept
	var payload *interface{}
	if aggConceptModel {
		concept, payload, err = h.processAggregateConceptModel(uuid, conceptType, body)
	} else {
		concept, payload, err = h.processConceptModel(uuid, conceptType, body)
	}

	return conceptType, concept, payload, err
}

func (h *Handler) processConceptModel(uuid string, conceptType string, body []byte) (service.Concept, *interface{}, error) {
	var concept service.ConceptModel
	err := json.Unmarshal(body, &concept)
	if err != nil {
		log.WithError(err).Info("Failed to unmarshal body into concept model.")
		return nil, nil, errProcessingBody
	}

	if concept.UUID != uuid {
		return nil, nil, errPathUUID
	}

	if concept.DirectType == "" || concept.PrefLabel == "" {
		return nil, nil, errInvalidConceptModel
	}

	payload := h.modelPopulator.ConvertConceptToESConceptModel(concept, conceptType)
	return concept, &payload, nil
}

func (h *Handler) processAggregateConceptModel(uuid string, conceptType string, body []byte) (service.Concept, *interface{}, error) {
	var concept service.AggregateConceptModel
	err := json.Unmarshal(body, &concept)
	if err != nil {
		log.WithError(err).Info("Failed to unmarshal body into aggregate concept model.")
		return nil, nil, errProcessingBody
	}

	if concept.PrefUUID != uuid {
		return nil, nil, errPathUUID
	}

	if concept.DirectType == "" || concept.PrefLabel == "" {
		return nil, nil, errInvalidConceptModel
	}

	payload := h.modelPopulator.ConvertAggregateConceptToESConceptModel(concept, conceptType)
	return concept, &payload, nil
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

// DeleteData handles a delete for a concept
func (h *Handler) DeleteData(writer http.ResponseWriter, request *http.Request) {
	transactionID := tid.GetTransactionIDFromRequest(request)
	ctx := tid.TransactionAwareContext(context.Background(), transactionID)

	uuid := mux.Vars(request)["id"]
	conceptType := mux.Vars(request)["concept-type"]

	res, err := h.elasticService.DeleteData(conceptType, uuid, ctx)

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

// Close terminates the underlying ES bulk processor
func (h *Handler) Close() {
	h.elasticService.CloseBulkProcessor()
}

func writeMessage(w http.ResponseWriter, msg string, status int) {
	w.Header().Add("Content-Type", "application/json")
	data, _ := json.Marshal(&struct {
		Msg string `json:"message"`
	}{msg})

	w.WriteHeader(status)
	w.Write(data)
}

func isAggregateConceptModel(body []byte) (bool, error) {
	data := make(map[string]interface{})
	err := json.Unmarshal(body, &data)
	if err != nil {
		return false, err
	}

	_, ok := data["prefUUID"]
	return ok, nil
}
