package main

import (
	"errors"
	"gopkg.in/olivere/elastic.v3"
)

type esHealthService struct {
	client *elastic.Client
}

type esHealthServiceI interface {
	getClusterHealth() (*elastic.ClusterHealthResponse, error)
}

func (esHealthService esHealthService) getClusterHealth() (*elastic.ClusterHealthResponse, error) {
	if esHealthService.client == nil {
		return nil, errors.New("Client could not be created, please check the application parameters/env variables, and restart the service.")
	}

	return esHealthService.client.ClusterHealth().Do()
}

func newEsHealthService(client *elastic.Client) *esHealthService {
	return &esHealthService{client: client}
}
