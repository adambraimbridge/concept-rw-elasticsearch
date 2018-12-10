package service

import (
	log "github.com/Financial-Times/go-logger"
	"github.com/smartystreets/go-aws-auth"
	"gopkg.in/olivere/elastic.v5"
	"net/http"
	"net/url"
)

type EsAccessConfig struct {
	accessKey    string
	secretKey    string
	esEndpoint   string
	traceLogging bool
}

func NewAccessConfig(accessKey string, secretKey string, endpoint string, tracelogging bool) EsAccessConfig {
	return EsAccessConfig{accessKey: accessKey, secretKey: secretKey, esEndpoint: endpoint, traceLogging: tracelogging}
}

type AWSSigningTransport struct {
	HTTPClient  *http.Client
	Credentials awsauth.Credentials
	Host string
}

// RoundTrip implementation
func (a AWSSigningTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Host = a.Host
	return a.HTTPClient.Do(awsauth.Sign4(req, a.Credentials))
}

func newAmazonClient(config EsAccessConfig) (*elastic.Client, error) {
	u, err := url.Parse(config.esEndpoint)
	if err != nil {
		log.Errorf("Failed to parse host=[%v]", err)
	}

	signingTransport := AWSSigningTransport{
		Credentials: awsauth.Credentials{
			AccessKeyID:     config.accessKey,
			SecretAccessKey: config.secretKey,
		},
		HTTPClient: http.DefaultClient,
		Host: u.Host,
	}
	signingClient := &http.Client{Transport: http.RoundTripper(signingTransport)}

	log.Infof("connecting with AWSSigningTransport to %s", config.esEndpoint)
	return newClient(config.esEndpoint, config.traceLogging,
		elastic.SetScheme("https"),
		elastic.SetHttpClient(signingClient),
	)
}

func newSimpleClient(config EsAccessConfig) (*elastic.Client, error) {
	log.Infof("connecting with default transport to %s", config.esEndpoint)
	return newClient(config.esEndpoint, config.traceLogging)
}

func newClient(endpoint string, traceLogging bool, options ...elastic.ClientOptionFunc) (*elastic.Client, error) {
	optionFuncs := []elastic.ClientOptionFunc{
		elastic.SetURL(endpoint),
		elastic.SetSniff(false), //needs to be disabled due to EAS behavior. Healthcheck still operates as normal.
	}
	optionFuncs = append(optionFuncs, options...)

	if traceLogging {
		optionFuncs = append(optionFuncs, elastic.SetTraceLog(log.Logger()))
	}

	return elastic.NewClient(optionFuncs...)
}

func NewElasticClient(region string, config EsAccessConfig) (*elastic.Client, error) {
	if region == "local" {
		return newSimpleClient(config)
	} else {
		return newAmazonClient(config)
	}
}
