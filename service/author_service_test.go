package service

import (
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

var expectedAuthorUUIDs = map[string]struct{}{
	"2916ded0-6d1f-4449-b54c-3805da729c1d": struct{}{},
	"ddc22d37-624a-4a3d-88e5-ba508e38c8ba": struct{}{},
}

var authorIds = "{\"ID\":\"2916ded0-6d1f-4449-b54c-3805da729c1d\"}\n{\"ID\":\"ddc22d37-624a-4a3d-88e5-ba508e38c8ba\"}"

func (m *mockAuthorTransformerServer) startMockAuthorTransformerServer(t *testing.T) *httptest.Server {
	m.countLock = &sync.RWMutex{}
	r := mux.NewRouter()
	r.HandleFunc(authorTransformerIdsPath, func(w http.ResponseWriter, req *http.Request) {
		ua := req.Header.Get("User-Agent")
		assert.Equal(t, "UPP concept-rw-elasticsearch", ua, "user-agent header")
		tid := req.Header.Get("X-Request-Id")
		assert.NotEmpty(t, tid, "transaction id")
		contentType := req.Header.Get("Content-Type")
		user, password, _ := req.BasicAuth()

		status, resp := m.Ids(contentType, user, password)
		w.WriteHeader(status)
		w.Write([]byte(resp))
		m.countLock.Lock()
		defer m.countLock.Unlock()
		m.count++
	}).Methods("GET")

	r.HandleFunc(gtgPath, func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(m.GTG())
	})

	return httptest.NewServer(r)
}

func TestLoadAuthorIdentifiersResponseSuccess(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	as, err := NewAuthorService(testServer.URL, "username:password", time.Minute*60, &http.Client{})
	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")

	for expectedUUID := range expectedAuthorUUIDs {
		actual, err := as.IsFTAuthor(expectedUUID)
		assert.True(t, actual, "The UUID is not in the author uuid map")
		assert.NoError(t, err, "Checking for FTAuthor should not return an error")
	}
	m.AssertExpectations(t)
}

func TestLoadAuthorIdentifiersResponseNot200(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusBadRequest, "")

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	_, err := NewAuthorService(testServer.URL, "username:password", time.Minute*60, &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)

}

func TestLoadAuthorIdentifiersRequestError(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	_, err := NewAuthorService("#:", "username:password", time.Minute*60, &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)

}

func TestLoadAuthorIdentifiersResponseError(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	testServer := m.startMockAuthorTransformerServer(t)
	testServer.Close()

	_, err := NewAuthorService(testServer.URL, "username:password", time.Minute*60, &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)
}

func TestLoadAuthorServiceMissingRequestCredentials(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	testServer := m.startMockAuthorTransformerServer(t)
	testServer.Close()

	_, err := NewAuthorService(testServer.URL, "", time.Minute*60, &http.Client{})
	assert.Error(t, err)
	m.AssertExpectations(t)
}

func TestRefreshAuthorIdentifiersResponseSuccess(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)
	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	as, err := NewAuthorService(testServer.URL, "username:password", time.Millisecond*10, &http.Client{})

	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")
	for expectedUUID := range expectedAuthorUUIDs {
		actual, err := as.IsFTAuthor(expectedUUID)
		assert.True(t, actual, "The UUID is not in the author uuid map")
		assert.NoError(t, err, "Checking for FTAuthor should not return an error")
	}

	as.RefreshAuthorIdentifiers()
	defer as.(*curatedAuthorService).refreshTicker.Stop()

	time.Sleep(time.Millisecond * 35)
	m.countLock.RLock()
	defer m.countLock.RUnlock()
	assert.True(t, m.count >= 4, "author list is not being refreshed correctly, number of calls made: "+strconv.Itoa(m.count))
	m.AssertExpectations(t)
}

func TestRefreshAuthorIdentifiersWithErrorContinues(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)
	m.ExpectedCalls = make([]*mock.Call, 0)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusBadRequest, "")
	m.ExpectedCalls = make([]*mock.Call, 0)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)
	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	as, err := NewAuthorService(testServer.URL, "username:password", time.Millisecond*10, &http.Client{})

	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")
	for expectedUUID := range expectedAuthorUUIDs {
		actual, err := as.IsFTAuthor(expectedUUID)
		assert.True(t, actual, "The UUID is not in the author uuid map")
		assert.NoError(t, err, "Checking for FTAuthor should not return an error")
	}
	as.RefreshAuthorIdentifiers()
	defer as.(*curatedAuthorService).refreshTicker.Stop()

	time.Sleep(time.Millisecond * 35)
	m.countLock.RLock()
	defer m.countLock.RUnlock()
	assert.True(t, m.count >= 4, "author list is not being refreshed correctly, number of calls made:"+strconv.Itoa(m.count))

	m.AssertExpectations(t)
}

func TestIsFTAuthorTrue(t *testing.T) {
	testService := &curatedAuthorService{
		httpClient:  nil,
		serviceURL:  "url",
		authorUUIDs: expectedAuthorUUIDs,
		authorLock:  &sync.RWMutex{},
	}
	isAuthor, err := testService.IsFTAuthor("2916ded0-6d1f-4449-b54c-3805da729c1d")
	assert.True(t, isAuthor)
	assert.NoError(t, err, "Checking for FTAuthor should not return an error")
}

func TestIsIsFTAuthorFalse(t *testing.T) {
	testService := &curatedAuthorService{
		httpClient:  nil,
		serviceURL:  "url",
		authorUUIDs: expectedAuthorUUIDs,
		authorLock:  &sync.RWMutex{},
	}
	isAuthor, err := testService.IsFTAuthor("61346cf7-008b-49e0-945a-832a90cd60ac")
	assert.False(t, isAuthor)
	assert.NoError(t, err, "Checking for FTAuthor should not return an error")
}

func TestIsIsFTAuthorEmptyList(t *testing.T) {
	testService := &curatedAuthorService{
		httpClient:  nil,
		serviceURL:  "url",
		authorUUIDs: map[string]struct{}{},
		authorLock:  &sync.RWMutex{},
	}
	_, err := testService.IsFTAuthor("61346cf7-008b-49e0-945a-832a90cd60ac")
	assert.EqualError(t, err, "Author list is unavailable", "Checking for FTAuthor should return an error")
}

func TestIsGTG(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)
	m.On("GTG").Return(http.StatusOK)

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	as, err := NewAuthorService(testServer.URL, "username:password", time.Minute*60, &http.Client{})
	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")
	assert.NoError(t, as.IsGTG(), "No GTG errors")
}

func TestIsNotGTG(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)
	m.On("GTG").Return(http.StatusServiceUnavailable)

	testServer := m.startMockAuthorTransformerServer(t)
	defer testServer.Close()

	as, err := NewAuthorService(testServer.URL, "username:password", time.Minute*60, &http.Client{})
	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")
	assert.EqualError(t, as.IsGTG(), "gtg endpoint returned a non-200 status: 503", "GTG should return 503")
}

func TestGTGConnectionError(t *testing.T) {
	m := new(mockAuthorTransformerServer)
	m.On("Ids", "application/json", "username", "password").Return(http.StatusOK, authorIds)
	m.On("GTG").Return(http.StatusServiceUnavailable)

	testServer := m.startMockAuthorTransformerServer(t)

	as, err := NewAuthorService(testServer.URL, "username:password", time.Minute*60, &http.Client{})
	assert.NoError(t, err, "Creation of a new Author sevice should not return an error")
	testServer.Close()
	assert.Error(t, as.IsGTG(), "GTG should return a connection error")
}

type mockAuthorTransformerServer struct {
	mock.Mock
	count     int
	countLock *sync.RWMutex
}

func (m *mockAuthorTransformerServer) Ids(contentType string, user string, password string) (int, string) {
	args := m.Called(contentType, user, password)
	return args.Int(0), args.String(1)
}

func (m *mockAuthorTransformerServer) GTG() int {
	args := m.Called()
	return args.Int(0)
}
