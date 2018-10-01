package service

// Concept contains common function between both concept models
type Concept interface {
	// GetAuthorities returns an array containing all authorities that this concept is identified by
	GetAuthorities() []string
	// ConcordedUUIDs returns an array containing all concorded concept uuids - N.B. it will not contain the canonical prefUUID.
	ConcordedUUIDs() []string

	PreferredUUID() string
}

type ConceptModel struct {
	UUID                   string                 `json:"uuid"`
	DirectType             string                 `json:"type"`
	PrefLabel              string                 `json:"prefLabel"`
	Authority              string                 `json:"authority,omitempty"`
	Aliases                []string               `json:"aliases,omitempty"`
	AlternativeIdentifiers map[string]interface{} `json:"alternativeIdentifiers,omitempty"`
	IsDeprecated           bool                   `json:"isDeprecated,omitempty"`
	ScopeNote              string                 `json:"scopeNote,omitempty"`
}

type AggregateConceptModel struct {
	PrefUUID              string          `json:"prefUUID"`
	DirectType            string          `json:"type"`
	PrefLabel             string          `json:"prefLabel"`
	Aliases               []string        `json:"aliases,omitempty"`
	SourceRepresentations []SourceConcept `json:"sourceRepresentations"`
	IsAuthor              bool            `json:"isAuthor"`
	IsDeprecated          bool            `json:"isDeprecated,omitempty"`
	ScopeNote             string          `json:"scopeNote,omitempty"`
}

type SourceConcept struct {
	UUID      string `json:"uuid"`
	Authority string `json:"authority"`
}

type EsConceptModel struct {
	Id               string          `json:"id"`
	ApiUrl           string          `json:"apiUrl"`
	PrefLabel        string          `json:"prefLabel"`
	Types            []string        `json:"types"`
	Authorities      []string        `json:"authorities"`
	DirectType       string          `json:"directType"`
	Aliases          []string        `json:"aliases,omitempty"`
	LastModified     string          `json:"lastModified"`
	PublishReference string          `json:"publishReference"`
	IsDeprecated     bool            `json:"isDeprecated,omitempty"` // stored only if this is true
	ScopeNote        string          `json:"scopeNote,omitempty"`
	Metrics          *ConceptMetrics `json:"metrics,omitempty"`
}

type EsIDTypePair struct {
	ID   string `json:"id,omitempty"`
	Type string `json:"type,omitempty"`
}

type MetricsPayload struct {
	Metrics *ConceptMetrics `json:"metrics"`
}

type ConceptMetrics struct {
	AnnotationsCount int `json:"annotationsCount"`
}

type EsPersonConceptModel struct {
	*EsConceptModel
	IsFTAuthor string `json:"isFTAuthor"`
}

func (c AggregateConceptModel) PreferredUUID() string {
	return c.PrefUUID
}

func (c ConceptModel) PreferredUUID() string {
	return c.UUID
}

func (c ConceptModel) GetAuthorities() []string {
	var authorities []string

	if c.AlternativeIdentifiers == nil && c.Authority != "" {
		return []string{c.Authority}
	}

	for authority := range c.AlternativeIdentifiers {
		if authority == "uuids" {
			continue // exclude the "uuids" alternativeIdentifier
		}
		authorities = append(authorities, authority)
	}
	return authorities
}

func (c AggregateConceptModel) GetAuthorities() []string {
	var authorities []string
	for _, src := range c.SourceRepresentations {
		authorities = append(authorities, src.Authority)
	}
	return authorities
}

func (c ConceptModel) ConcordedUUIDs() []string {
	return make([]string, 0) // we don't want to remove concorded concepts for the original concept model.
}

func (c AggregateConceptModel) ConcordedUUIDs() []string {
	var uuids []string
	for _, src := range c.SourceRepresentations {
		if src.UUID != c.PrefUUID {
			uuids = append(uuids, src.UUID)
		}
	}
	return uuids
}
