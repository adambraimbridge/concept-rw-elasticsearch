package main

import (
	"github.com/Financial-Times/neo-model-utils-go/mapper"
)

type conceptModel struct {
	UUID       string   `json:"uuid"`
	DirectType string   `json:"type"`
	PrefLabel  string   `json:"prefLabel"`
	Aliases    []string `json:"aliases,omitempty"`
	Types      []string `json:"types,omitempty"`
}

type esConceptModel struct {
	Id         string   `json:"id"`
	ApiUrl     string   `json:"apiUrl"`
	PrefLabel  string   `json:"prefLabel"`
	Types      []string `json:"types"`
	DirectType string   `json:"directType"`
	Aliases    []string `json:"aliases"`
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
