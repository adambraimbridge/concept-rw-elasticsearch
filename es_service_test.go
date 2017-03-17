package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNoElasticClient(t *testing.T) {
	service := esService{nil, nil, "test"}

	_, err := service.readData("any", "any")

	assert.Equal(t, ErrNoElasticClient, err, "error response")
}
