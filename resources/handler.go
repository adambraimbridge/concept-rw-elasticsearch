package resources

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
)

var (
	errPathUUID               = errors.New("Provided path UUID does not match request body")
	errInvalidConceptModel    = errors.New("Invalid or incomplete concept model")
	errUnsupportedConceptType = errors.New("Unsupported or invalid concept type")
	errProcessingBody         = errors.New("Request body is not in the expected concept model format")
)

// Handler handles http calls
type Handler struct {
	elasticService      service.EsServiceI
	allowedConceptTypes map[string]bool
}

// NewHandler creates a new http resource handler for the service
func NewHandler(elasticService service.EsServiceI, allowedConceptTypes []string) (service *Handler) {
	allowedTypes := make(map[string]bool)
	for _, v := range allowedConceptTypes {
		allowedTypes[v] = true
	}

	return &Handler{elasticService: elasticService, allowedConceptTypes: allowedTypes}
}

// LoadData processes a single ES concept entity
func (h *Handler) LoadData(w http.ResponseWriter, r *http.Request) {
	uuid, conceptType, payload, err := h.processPayload(w, r)
	if err == errUnsupportedConceptType || err == errInvalidConceptModel || err == errPathUUID || err == errProcessingBody {
		writeMessage(w, err.Error(), http.StatusBadRequest)
		return
	}

	_, err = h.elasticService.LoadData(conceptType, uuid, *payload)
	if err == service.ErrNoElasticClient {
		writeMessage(w, "ES unavailable", http.StatusServiceUnavailable)
		return
	}

	if err != nil {
		log.WithError(err).Warn("Failed to write data to elasticsearch.")
		writeMessage(w, "Failed to write data to ES", http.StatusInternalServerError)
		return
	}

	writeMessage(w, "Concept written successfully", http.StatusOK)
}

// LoadBulkData write a concept to ES via the ES Bulk API
func (h *Handler) LoadBulkData(w http.ResponseWriter, r *http.Request) {
	uuid, conceptType, payload, err := h.processPayload(w, r)
	if err == errUnsupportedConceptType || err == errInvalidConceptModel || err == errPathUUID || err == errProcessingBody {
		writeMessage(w, err.Error(), http.StatusBadRequest)
		return
	}

	h.elasticService.LoadBulkData(conceptType, uuid, *payload)
	writeMessage(w, "Concept written successfully", http.StatusOK)
}

func (h *Handler) processPayload(w http.ResponseWriter, r *http.Request) (string, string, *service.EsConceptModel, error) {
	vars := mux.Vars(r)
	uuid := vars["id"]
	conceptType := vars["concept-type"]

	if !h.allowedConceptTypes[conceptType] {
		return "", "", nil, errUnsupportedConceptType
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.WithError(err).Error("Failed to read request body")
		return "", "", nil, errProcessingBody
	}

	aggConceptModel, err := isAggregateConceptModel(body)
	if err != nil {
		log.WithError(err).Error("Failed to check if body json is an aggregate concept model or not")
		return "", "", nil, errProcessingBody
	}

	var payload *service.EsConceptModel
	if aggConceptModel {
		payload, err = processAggregateConceptModel(uuid, conceptType, body)
	} else {
		payload, err = processConceptModel(uuid, conceptType, body)
	}

	return uuid, conceptType, payload, err
}

func processConceptModel(uuid string, conceptType string, body []byte) (*service.EsConceptModel, error) {
	var concept service.ConceptModel
	err := json.Unmarshal(body, &concept)
	if err != nil {
		log.WithError(err).Info("Failed to unmarshal body into concept model.")
		return nil, errProcessingBody
	}

	if concept.UUID != uuid {
		return nil, errPathUUID
	}

	if concept.DirectType == "" || concept.PrefLabel == "" {
		return nil, errInvalidConceptModel
	}

	payload := service.ConvertConceptToESConceptModel(concept, conceptType)
	return &payload, nil
}

func processAggregateConceptModel(uuid string, conceptType string, body []byte) (*service.EsConceptModel, error) {
	var concept service.AggregateConceptModel
	err := json.Unmarshal(body, &concept)
	if err != nil {
		log.WithError(err).Info("Failed to unmarshal body into aggregate concept model.")
		return nil, errProcessingBody
	}

	if concept.PrefUUID != uuid {
		return nil, errPathUUID
	}

	if concept.DirectType == "" || concept.PrefLabel == "" {
		return nil, errInvalidConceptModel
	}

	payload := service.ConvertAggregateConceptToESConceptModel(concept, conceptType)
	return &payload, nil
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
