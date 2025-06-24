package service

import (
	"context"
	"fmt"
	"github.com/DIMO-Network/shared/pkg/db"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/test"
	"github.com/ethereum/go-ethereum/common"
	"github.com/patrickmn/go-cache"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"
)

const vin = "1GGCM82633A123456"

type OracleTestSuite struct {
	suite.Suite
	pdb       db.Store
	container testcontainers.Container
	ctx       context.Context
	cs        *OracleService
}

const migrationsDirRelPath = "../db/migrations"

// SetupSuite starts container db
func (s *OracleTestSuite) SetupSuite() {
	s.ctx = context.Background()
	s.pdb, s.container, _ = test.StartContainerDatabase(context.Background(), s.T(), migrationsDirRelPath)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})
	vehicleService := NewVehicleService(&s.pdb, &logger)
	s.cs = &OracleService{
		Ctx:    context.Background(),
		Db:     vehicleService,
		logger: logger,
	}
}

// TearDownTest after each test truncate tables
func (s *OracleTestSuite) TearDownTest() {
	test.TruncateTables(s.pdb.DBS().Writer.DB, s.T())
}

// TearDownSuite cleanup at end by terminating container
func (s *OracleTestSuite) TearDownSuite() {
	fmt.Printf("shutting down postgres at with session: %s \n", s.container.SessionID())
	if err := s.container.Terminate(s.ctx); err != nil {
		s.T().Fatal(err)
	}
}

func TestOracleTestSuite(t *testing.T) {
	suite.Run(t, new(OracleTestSuite))
}

var validCloudEventMsgNoProduceAndSubject = `{
  "id": "feeb4ceb-c2bb-42ee-bd01-b6024c2c391b",
  "source": "0xb83DE952D389f9A6806819434450324197712FDA",
  "producer": "",
  "specversion": "1.0",
  "subject": "",
  "time": "2025-03-04T12:00:00Z",
  "type": "dimo.status",
  "datacontenttype": "application/json",
  "dataversion": "default/v1.0",
  "data": {
    "signals": [
      {
        "name": "speed",
        "timestamp": "2025-03-04T12:01:00Z",
        "value": 55
      }
    ],
    "vin": "1GGCM82633A123456"
  }
}`

var validCloudEventMsg = `{
	  "id": "unique-event-identifier",
	  "source": "0xConnectionLicenseAddress",
	  "producer": "did:nft:1:0xbA5738a18d83D41847dfFbDC6101d37C69c9B0cF_42",
	  "specversion": "1.0",
	  "subject": "did:nft:1:0x123456789abcdef0123456789abcdef012345678_123",
	  "time": "2025-03-04T12:00:00Z",
	  "type": "dimo.status",
	  "datacontenttype": "application/json",
	  "dataversion": "default/v1.0",
	  "data": {
	    "signals": [
	      {
	        "name": "powertrainTransmissionTravelledDistance",
	        "timestamp": "2025-03-04T12:00:00Z",
	        "value": 12345.67
	      }
	    ],
	    "vin": "1GGCM82633A123456"
	  }
}`

var invalidCloudEventMsg = `{
	  "id": "unique-event-identifier",
	  "source": "0xConnectionLicenseAddress",
	  "specversion": "1.0",
	  "time": "2025-03-04T12:00:00Z",
	  "datacontenttype": "application/json",
	  "dataversion": "default/v1.0",
	  "data": {
	    "signals": [
	      {
	        "name": "powertrainTransmissionTravelledDistance",
	        "timestamp": "2025-03-04T12:00:00Z",
	        "value": 12345.67
	      }
	    ],
	    "vin": "1GGCM82633A123456"
	  }
}`

func (s *OracleTestSuite) TestDeviceMinted() {
	// given
	server, _ := setupMockServer(s.T())
	defer server.Close()

	oracleService := setupOracleService(server.URL)
	oracleService.Db = s.cs.Db
	dbVin := dbmodels.Vin{
		Vin:              vin,
		VehicleTokenID:   null.Int64From(456),
		SyntheticTokenID: null.Int64From(789),
		ExternalID:       null.StringFrom("ffbf0b52-d478-4320-9a1c-3b83f547f33b"),
		ConnectionStatus: null.StringFrom("succeeded"),
	}

	// when
	require.NoError(s.T(), dbVin.Insert(s.ctx, s.pdb.DBS().Writer, boil.Infer()))
	// Mock the IdentityAPIService
	mockService := &MockIdentityAPIService{
		MockGetCachedVehicleByTokenID: func(tokenID int64) (*models.Vehicle, error) {
			return &models.Vehicle{ID: "123", TokenID: tokenID}, nil
		},
		MockFetchVehicleByTokenID: func(tokenID int64) (*models.Vehicle, error) {
			return &models.Vehicle{ID: "456", TokenID: tokenID}, nil
		},
	}
	oracleService.identityService = mockService

	// then
	err := oracleService.HandleDeviceByVIN([]byte(validCloudEventMsgNoProduceAndSubject))

	// verify
	require.NoError(s.T(), err)
}

func (s *OracleTestSuite) TestDeviceNotFound() {
	// given
	server, _ := setupMockServer(s.T())
	defer server.Close()

	oracleService := setupOracleService(server.URL)
	oracleService.Db = s.cs.Db

	// when

	// then
	err := oracleService.HandleDeviceByVIN([]byte(validCloudEventMsgNoProduceAndSubject))

	// verify
	require.Error(s.T(), err)
}

func (s *OracleTestSuite) TestDeviceNotMinted() {
	// given
	server, _ := setupMockServer(s.T())
	defer server.Close()

	oracleService := setupOracleService(server.URL)
	oracleService.Db = s.cs.Db
	dbVin := dbmodels.Vin{
		Vin:              vin,
		ExternalID:       null.StringFrom("ffbf0b52-d478-4320-9a1c-3b83f547f33b"),
		ConnectionStatus: null.StringFrom("succeeded"),
	}

	// when
	require.NoError(s.T(), dbVin.Insert(s.ctx, s.pdb.DBS().Writer, boil.Infer()))

	// then
	// should not send msg to DIS but not fail
	err := oracleService.HandleDeviceByVIN([]byte(validCloudEventMsgNoProduceAndSubject))

	// verify
	require.NoError(s.T(), err)
}

func (s *OracleTestSuite) TestDeviceSendValidCloudEvent() {
	// given
	server, _ := setupMockServer(s.T())
	defer server.Close()

	oracleService := setupOracleService(server.URL)
	oracleService.Db = s.cs.Db
	dbVin := dbmodels.Vin{
		Vin:              vin,
		ExternalID:       null.StringFrom("ffbf0b52-d478-4320-9a1c-3b83f547f33b"),
		ConnectionStatus: null.StringFrom("succeeded"),
	}

	// when
	require.NoError(s.T(), dbVin.Insert(s.ctx, s.pdb.DBS().Writer, boil.Infer()))

	// then
	// should not send msg to DIS but not fail
	err := oracleService.HandleDeviceByVIN([]byte(validCloudEventMsg))

	// verify
	require.NoError(s.T(), err)
}

func (s *OracleTestSuite) TestDeviceSendInvalidCloudEvent() {
	// given
	server, _ := setupMockServer(s.T())
	defer server.Close()

	oracleService := setupOracleService(server.URL)
	oracleService.Db = s.cs.Db
	dbVin := dbmodels.Vin{
		Vin:              vin,
		ExternalID:       null.StringFrom("ffbf0b52-d478-4320-9a1c-3b83f547f33b"),
		ConnectionStatus: null.StringFrom("succeeded"),
	}

	// when
	require.NoError(s.T(), dbVin.Insert(s.ctx, s.pdb.DBS().Writer, boil.Infer()))

	// then
	// should not send msg to DIS but not fail
	err := oracleService.HandleDeviceByVIN([]byte(invalidCloudEventMsg))

	// verify
	require.Error(s.T(), err)
}

// Setup code for the mock server
func setupMockServer(t *testing.T) (*httptest.Server, *int) {
	var callCount int
	server := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		callCount++ // Increment the counter each time the handler is called
		if req.URL.String() != "/status" {
			t.Errorf("request url should be %s but got %s", "dimoNodeEndpoint", req.URL.String())
		}
		_, err := rw.Write([]byte(`{"status": "ok"}`))
		if err != nil {
			t.Errorf("error writing response: %v", err)
		}
	}))
	return server, &callCount
}

// Setup code for the OracleService
func setupOracleService(serverURL string) *OracleService {
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr})
	dn := prepare(serverURL + "/status")
	identityURL, _ := url.Parse("https://identity-api.dimo.zone/query")
	settings := config.Settings{IdentityAPIEndpoint: *identityURL, ChainID: 1,
		VehicleNftAddress:   common.HexToAddress("0x123456789abcdef0123456789abcdef012345678"),
		SyntheticNftAddress: common.HexToAddress("0x123456789abcdef0123456789abcdef012345678")}
	c := cache.New(10*time.Minute, 15*time.Minute)
	cs := &OracleService{
		Ctx:             context.Background(),
		dimoNodeAPISvc:  dn,
		logger:          logger,
		settings:        settings,
		identityService: NewIdentityAPIService(logger, settings),
		stop:            make(chan bool),
		cache:           c,
	}
	return cs
}

func prepare(nodeEndpoint string) DimoNodeAPI {
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("app", "aftermarket-Adapter").
		Logger()

	settings := config.Settings{
		Environment:      "dev",
		DimoNodeEndpoint: nodeEndpoint,
		// Auto generate certs, not real
		Cert:    "-----BEGIN CERTIFICATE-----\nMIICDzCCAbagAwIBAgIUKD2IZB73Nxq22lTBdCYesZhLItMwCgYIKoZIzj0EAwIw\nZjELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBVN0YXRlMQ0wCwYDVQQHDARDaXR5MRUw\nEwYDVQQKDAxPcmdhbml6YXRpb24xEDAOBgNVBAsMB09yZ1VuaXQxDzANBgNVBAMM\nBlJvb3RDQTAeFw0yNDExMjUxNDMwNDZaFw0yNTExMjUxNDMwNDZaMGYxCzAJBgNV\nBAYTAlVTMQ4wDAYDVQQIDAVTdGF0ZTENMAsGA1UEBwwEQ2l0eTEVMBMGA1UECgwM\nT3JnYW5pemF0aW9uMRAwDgYDVQQLDAdPcmdVbml0MQ8wDQYDVQQDDAZTZXJ2ZXIw\nWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQQ2LpCc+E/eVUq0iDp4v4iHwhjw1ED\nVAu0ulEtkPD3wqwHQO9WnkfDr/LUDX9esekzpG92G2Aj4QA3/ZHagkmco0IwQDAd\nBgNVHQ4EFgQUSoQuTg2QLASJtia2FDdIrwkwEBMwHwYDVR0jBBgwFoAU6T9lFewD\n56/tMtIWA8ZMsj5568gwCgYIKoZIzj0EAwIDRwAwRAIgRvJ2u8fCgD0g+4J2cPFD\nnx5IlK7bxH1UavISJXOFVdECIFFxk+kymOq1dDwLmD+BXH5laIGe5oX4w6nh07hY\nbPh4\n-----END CERTIFICATE-----",
		CertKey: "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIBnRSSEPmLzO0BKSotgA7j8Ev/079M2OUNE+yqH6SVWRoAoGCCqGSM49\nAwEHoUQDQgAEENi6QnPhP3lVKtIg6eL+Ih8IY8NRA1QLtLpRLZDw98KsB0DvVp5H\nw6/y1A1/XrHpM6RvdhtgI+EAN/2R2oJJnA==\n-----END EC PRIVATE KEY-----",
		CACert:  "-----BEGIN CERTIFICATE-----\nMIICITCCAcegAwIBAgIUa3YulMTE/6FaHhYoKQy9xiIFfHkwCgYIKoZIzj0EAwIw\nZjELMAkGA1UEBhMCVVMxDjAMBgNVBAgMBVN0YXRlMQ0wCwYDVQQHDARDaXR5MRUw\nEwYDVQQKDAxPcmdhbml6YXRpb24xEDAOBgNVBAsMB09yZ1VuaXQxDzANBgNVBAMM\nBlJvb3RDQTAeFw0yNDExMjUxNDMwMjNaFw0zNDExMjMxNDMwMjNaMGYxCzAJBgNV\nBAYTAlVTMQ4wDAYDVQQIDAVTdGF0ZTENMAsGA1UEBwwEQ2l0eTEVMBMGA1UECgwM\nT3JnYW5pemF0aW9uMRAwDgYDVQQLDAdPcmdVbml0MQ8wDQYDVQQDDAZSb290Q0Ew\nWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQFmAJVlQbbf7xJunO79eLu4c1ztyii\nvnAmwSVKQ/ptRU5BAqTgRAmrI1RsZ3/Edztsed/P0rT8IKaTAp1OYhYJo1MwUTAd\nBgNVHQ4EFgQU6T9lFewD56/tMtIWA8ZMsj5568gwHwYDVR0jBBgwFoAU6T9lFewD\n56/tMtIWA8ZMsj5568gwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBF\nAiBksKW6dALWwOUXVK+bYJCTmC+8qUHwgzr83+WbsEPRzgIhAObRu7jtj1KUQ9CS\n2kChie/qJ8yVc1A/UexBn6IRXdYV\n-----END CERTIFICATE-----\n",
	}

	// Initialize the dimo node service
	dimoNodeAPISvc := NewDimoNodeAPIService(logger, settings)

	return dimoNodeAPISvc
}

// MockIdentityAPIService is a mock implementation of IdentityAPIService
type MockIdentityAPIService struct {
	MockGetCachedVehicleByTokenID    func(tokenID int64) (*models.Vehicle, error)
	MockFetchVehicleByTokenID        func(tokenID int64) (*models.Vehicle, error)
	MockFetchVehiclesByWalletAddress func(walletAddress string) ([]models.Vehicle, error)

	MockGetDeviceDefinitionByID       func(id string) (*models.DeviceDefinition, error)
	MockGetCachedDeviceDefinitionByID func(id string) (*models.DeviceDefinition, error)
	MockFetchDeviceDefinitionByID     func(id string) (*models.DeviceDefinition, error)
}

func (m *MockIdentityAPIService) GetCachedVehicleByTokenID(tokenID int64) (*models.Vehicle, error) {
	if m.MockGetCachedVehicleByTokenID != nil {
		return m.MockGetCachedVehicleByTokenID(tokenID)
	}
	return nil, nil
}

func (m *MockIdentityAPIService) FetchVehicleByTokenID(tokenID int64) (*models.Vehicle, error) {
	if m.MockFetchVehicleByTokenID != nil {
		return m.MockFetchVehicleByTokenID(tokenID)
	}
	return nil, nil
}

func (m *MockIdentityAPIService) FetchVehiclesByWalletAddress(walletAddress string) ([]models.Vehicle, error) {
	if m.MockFetchVehiclesByWalletAddress != nil {
		return m.MockFetchVehiclesByWalletAddress(walletAddress)
	}
	return nil, nil
}

func (m *MockIdentityAPIService) GetDeviceDefinitionByID(id string) (*models.DeviceDefinition, error) {
	if m.MockGetCachedDeviceDefinitionByID != nil {
		return m.MockGetCachedDeviceDefinitionByID(id)
	}
	return nil, nil
}

func (m *MockIdentityAPIService) GetCachedDeviceDefinitionByID(id string) (*models.DeviceDefinition, error) {
	if m.MockGetCachedDeviceDefinitionByID != nil {
		return m.MockGetCachedDeviceDefinitionByID(id)
	}
	return nil, nil
}

func (m *MockIdentityAPIService) FetchDeviceDefinitionByID(id string) (*models.DeviceDefinition, error) {
	if m.MockFetchDeviceDefinitionByID != nil {
		return m.MockFetchDeviceDefinitionByID(id)
	}
	return nil, nil
}
