package service

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	transactionid "github.com/Financial-Times/transactionid-utils-go"
	log "github.com/sirupsen/logrus"
)

const contentType = "application/json"
const authorTransformerIdsPath = "/__v1-authors-transformer/transformers/authors/__ids"
const gtgPath = "/__v1-authors-transformer/__gtg"

var (
	ErrNoAuthors = errors.New("Author list is unavailable")
)

type AuthorUUID struct {
	UUID string `json:"ID"`
}

type AuthorService interface {
	LoadAuthorIdentifiers() error
	RefreshAuthorIdentifiers()
	IsFTAuthor(UUID string) (bool, error)
	IsGTG() error
}

//uses v1 transformer author list
type curatedAuthorService struct {
	httpClient             *http.Client
	serviceURL             string
	authorUUIDs            map[string]struct{}
	authorLock             *sync.RWMutex
	publishClusterUser     string
	publishClusterpassword string
	refreshTicker          *time.Ticker
}

func NewAuthorService(serviceURL string, pubClusterKey string, authorRefreshInterval time.Duration, client *http.Client) (AuthorService, error) {
	if len(pubClusterKey) == 0 {
		return nil, fmt.Errorf("credentials missing credentials, author service cannot make request to author transformer")
	}
	credentials := strings.Split(pubClusterKey, ":")
	cas := &curatedAuthorService{
		client,
		serviceURL,
		nil,
		&sync.RWMutex{},
		credentials[0],
		credentials[1],
		time.NewTicker(authorRefreshInterval),
	}
	return cas, cas.LoadAuthorIdentifiers()
}

func (as *curatedAuthorService) LoadAuthorIdentifiers() error {
	tid := transactionid.NewTransactionID()
	req, err := http.NewRequest("GET", as.serviceURL+authorTransformerIdsPath, nil)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", contentType)
	req.Header.Add("X-Request-Id", tid)
	req.Header.Add("User-Agent", "UPP concept-rw-elasticsearch")
	req.SetBasicAuth(as.publishClusterUser, as.publishClusterpassword)
	log.WithField("transaction_id", tid).Info("Requesting author list from v1 authors transformer." + req.RequestURI)

	resp, err := as.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return fmt.Errorf("A non-200 error code from v1 authors transformer! Status: %v", resp.StatusCode)
	}

	authorUUIDsTmp := make(map[string]struct{})

	scan := bufio.NewScanner(resp.Body)
	for scan.Scan() {
		var id AuthorUUID
		err = json.Unmarshal(scan.Bytes(), &id)
		if err != nil {
			return err
		}
		authorUUIDsTmp[id.UUID] = struct{}{}
	}
	as.authorLock.Lock()
	defer as.authorLock.Unlock()
	as.authorUUIDs = authorUUIDsTmp
	log.Infof("Found %v authors", len(as.authorUUIDs))

	return nil
}

func (as *curatedAuthorService) RefreshAuthorIdentifiers() {
	go func() {
		for range as.refreshTicker.C {
			err := as.LoadAuthorIdentifiers()
			if err != nil { //log and use the map in memory
				log.Errorf("Error on author identifier list refresh attempt %v", err)
			} else {
				log.Infof("Author identifier list has been refreshed")
			}
		}
	}()

}

func (as *curatedAuthorService) IsFTAuthor(uuid string) (bool, error) {
	as.authorLock.RLock()
	defer as.authorLock.RUnlock()

	if len(as.authorUUIDs) == 0 {
		return false, ErrNoAuthors
	}

	_, found := as.authorUUIDs[uuid]

	return found, nil
}

func (as *curatedAuthorService) IsGTG() error {
	req, err := http.NewRequest("GET", as.serviceURL+gtgPath, nil)
	req.SetBasicAuth(as.publishClusterUser, as.publishClusterpassword)
	if err != nil {
		return err
	}
	resp, err := as.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gtg endpoint returned a non-200 status: %v", resp.StatusCode)
	}
	return nil
}
