package health

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Financial-Times/concept-rw-elasticsearch/service"
	"github.com/Financial-Times/go-fthealth/v1a"
	log "github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

type HealthService struct {
	esHealthService service.EsHealthServiceI
}

func NewHealthService(esHealthService service.EsHealthServiceI) *HealthService {
	return &HealthService{esHealthService: esHealthService}
}

func (service *HealthService) ClusterIsHealthyCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Full or partial degradation in serving requests from Elasticsearch",
		Name:             "Check Elasticsearch cluster health",
		PanicGuide:       "https://dewey.ft.com/up-crwes.html",
		Severity:         1,
		TechnicalSummary: "Elasticsearch cluster is not healthy. Details on /__health-details",
		Checker:          service.healthChecker,
	}
}

func (service *HealthService) healthChecker() (string, error) {
	output, err := service.esHealthService.GetClusterHealth()
	if err != nil {
		return "Cluster is not healthy: ", err
	} else if output.Status != "green" {
		return "Cluster is not healthy", errors.New(fmt.Sprintf("Cluster is %v", output.Status))
	} else {
		return "Cluster is healthy", nil
	}
}

func (service *HealthService) ConnectivityHealthyCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Could not connect to Elasticsearch",
		Name:             "Check connectivity to the Elasticsearch cluster",
		PanicGuide:       "https://dewey.ft.com/up-crwes.html",
		Severity:         1,
		TechnicalSummary: "Connection to Elasticsearch cluster could not be created. Please check your AWS credentials.",
		Checker:          service.connectivityChecker,
	}
}

func (service *HealthService) connectivityChecker() (string, error) {

	_, err := service.esHealthService.GetClusterHealth()
	if err != nil {
		return "Could not connect to elasticsearch", err
	}

	return "Successfully connected to the cluster", nil
}

//GoodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (service *HealthService) GoodToGo(writer http.ResponseWriter, req *http.Request) {
	if _, err := service.healthChecker(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
	}
}

//HealthDetails returns the response from elasticsearch service /__health endpoint - describing the cluster health
func (service *HealthService) HealthDetails(writer http.ResponseWriter, req *http.Request) {

	writer.Header().Set("Content-Type", "application/json")

	output, err := service.esHealthService.GetClusterHealth()
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}

	var response []byte
	response, err = json.Marshal(*output)
	if err != nil {
		response = []byte(err.Error())
	}

	_, err = writer.Write(response)
	if err != nil {
		log.Errorf(err.Error())
	}
}
