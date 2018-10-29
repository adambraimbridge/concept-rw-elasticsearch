package service

import (
	"time"

	"github.com/Financial-Times/neo-model-utils-go/mapper"
	log "github.com/sirupsen/logrus"
)

const (
	PERSON = "people"
)

type ModelPopulator interface {
	ConvertConceptToESConceptModel(concept ConceptModel, conceptType string, publishRef string) (interface{}, error)
	ConvertAggregateConceptToESConceptModel(concept AggregateConceptModel, conceptType string, publishRef string) (interface{}, error)
}

func ConvertConceptToESConceptModel(concept ConceptModel, conceptType string, publishRef string) (interface{}, error) {
	esModel := newESConceptModel(concept.UUID, conceptType, concept.DirectType, concept.Aliases, concept.GetAuthorities(), concept.PrefLabel, publishRef, concept.IsDeprecated, concept.ScopeNote)

	switch conceptType {
	case PERSON: // person type should not come through as the old model.
		esPersonModel := &EsPersonConceptModel{
			EsConceptModel: esModel,
		}
		return esPersonModel, nil
	default:
		return esModel, nil
	}
}

func ConvertAggregateConceptToESConceptModel(concept AggregateConceptModel, conceptType string, publishRef string) (interface{}, error) {
	esModel := newESConceptModel(concept.PrefUUID, conceptType, concept.DirectType, concept.Aliases, concept.GetAuthorities(), concept.PrefLabel, publishRef, concept.IsDeprecated, concept.ScopeNote)
	switch conceptType {
	case PERSON:
		esPersonModel := &EsPersonConceptModel{
			EsConceptModel: esModel,
		}
		return esPersonModel, nil
	default:
		return esModel, nil
	}
}

func newESConceptModel(uuid string, conceptType string, directType string, aliases []string, authorities []string, prefLabel string, publishRef string, isDeprecated bool, scopeNote string) *EsConceptModel {
	esModel := &EsConceptModel{}
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
	esModel.LastModified = time.Now().Format(time.RFC3339)
	esModel.PublishReference = publishRef
	esModel.IsDeprecated = isDeprecated
	esModel.ScopeNote = scopeNote

	return esModel
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
