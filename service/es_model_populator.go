package service

import (
	"strconv"

	"github.com/Financial-Times/neo-model-utils-go/mapper"
	log "github.com/Sirupsen/logrus"
)

const (
	PERSON = "people"
)

type ModelPopulator interface {
	ConvertConceptToESConceptModel(concept ConceptModel, conceptType string) interface{}
	ConvertAggregateConceptToESConceptModel(concept AggregateConceptModel, conceptType string) interface{}
}

type EsModelPopulator struct {
	authorService AuthorService
}

func NewEsModelPopulator(authorService AuthorService) ModelPopulator {
	return &EsModelPopulator{authorService}
}

func (mp *EsModelPopulator) ConvertConceptToESConceptModel(concept ConceptModel, conceptType string) interface{} {
	esModel := convertToESConceptModel(concept, conceptType)

	switch conceptType {
	case PERSON:
		return mp.convertToESPersonConceptModel(esModel, concept.UUID, conceptType)
	default:
		return esModel
	}
}

func (mp *EsModelPopulator) ConvertAggregateConceptToESConceptModel(concept AggregateConceptModel, conceptType string) interface{} {
	esModel := convertAggregateConceptToESConceptModel(concept, conceptType)

	switch conceptType {
	case PERSON:
		return mp.convertToESPersonConceptModel(esModel, concept.PrefUUID, conceptType)
	default:
		return esModel
	}
}

func convertAggregateConceptToESConceptModel(concept AggregateConceptModel, conceptType string) EsConceptModel {
	return newESConceptModel(concept.PrefUUID, conceptType, concept.DirectType, concept.Aliases, concept.GetAuthorities(), concept.PrefLabel)
}

func convertToESConceptModel(concept ConceptModel, conceptType string) EsConceptModel {
	return newESConceptModel(concept.UUID, conceptType, concept.DirectType, concept.Aliases, concept.GetAuthorities(), concept.PrefLabel)
}

func newESConceptModel(uuid string, conceptType string, directType string, aliases []string, authorities []string, prefLabel string) EsConceptModel {
	esModel := EsConceptModel{}
	esModel.ApiUrl = mapper.APIURL(uuid, []string{directType}, "")
	esModel.Id = mapper.IDURL(uuid)
	esModel.Types = mapper.TypeURIs(getTypes(directType))
	directTypeArray := mapper.TypeURIs([]string{directType})
	if len(directTypeArray) == 1 {
		esModel.DirectType = directTypeArray[0]
	} else {
		log.WithField("conceptType", conceptType).WithField("prefUUID", uuid).Warn("More than one directType found during type mapping.")
	}

	esModel.Aliases = aliases
	esModel.PrefLabel = prefLabel
	esModel.Authorities = authorities
	return esModel
}

func (mp *EsModelPopulator) convertToESPersonConceptModel(esConceptModel EsConceptModel, uuid string, conceptType string) EsPersonConceptModel {
	esPersonModel := EsPersonConceptModel{
		esConceptModel,
		strconv.FormatBool(mp.authorService.IsFTAuthor(uuid)),
	}
	return esPersonModel
}

func getTypes(conceptType string) []string {
	conceptTypes := []string{conceptType}
	parentType := mapper.ParentType(conceptType)
	for parentType != "" {
		conceptTypes = append(conceptTypes, parentType)
		parentType = mapper.ParentType(parentType)
	}
	return reverse(conceptTypes)
}

func reverse(strings []string) []string {
	if strings == nil {
		return nil
	}
	if len(strings) == 0 {
		return strings
	}
	var reversed []string
	for i := len(strings) - 1; i >= 0; i = i - 1 {
		reversed = append(reversed, strings[i])
	}
	return reversed
}
