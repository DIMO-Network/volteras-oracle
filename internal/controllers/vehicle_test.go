package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DIMO-Network/shared/pkg/db"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/mocks"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/onboarding"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/DIMO-Network/volteras-oracle/internal/test"
	"github.com/gofiber/fiber/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"gotest.tools/v3/assert"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

type VehicleControllerTestSuite struct {
	suite.Suite
	pdb          db.Store
	container    testcontainers.Container
	ctx          context.Context
	river        *river.Client[pgx.Tx]
	verifyWorker river.Worker[onboarding.VerifyArgs]
	settings     config.Settings
	vs           *service.Vehicle
	logger       *zerolog.Logger
}

const migrationsDirRelPath = "../db/migrations"

// SetupSuite starts container db
func (s *VehicleControllerTestSuite) SetupSuite() {
	s.ctx = context.Background()
	s.pdb, s.container, s.settings = test.StartContainerDatabase(context.Background(), s.T(), migrationsDirRelPath)
	s.vs = service.NewVehicleService(&s.pdb, s.logger)

	workers := river.NewWorkers()

	s.verifyWorker = mocks.NewVerifyWorkerMock()
	err := river.AddWorkerSafely(workers, s.verifyWorker)
	if err != nil {
		s.T().Fatal(err)
	}

	dbURL := s.settings.DB.BuildConnectionString(true)
	dbPool, _ := pgxpool.New(s.ctx, dbURL)

	riverClient, _ := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 10},
		},
		Workers: workers,
	})

	s.river = riverClient

	fmt.Println("Suite setup completed.")
}

// TearDownTest after each test truncate tables
func (s *VehicleControllerTestSuite) TearDownTest() {
	fmt.Println("Truncating database ...")
	test.TruncateTables(s.pdb.DBS().Writer.DB, s.T())
}

// TearDownSuite cleanup at end by terminating container
func (s *VehicleControllerTestSuite) TearDownSuite() {
	fmt.Printf("shutting down postgres at with session: %s \n", s.container.SessionID())
	if err := s.container.Terminate(s.ctx); err != nil {
		s.T().Fatal(err)
	}

	if err := s.river.Stop(s.ctx); err != nil {
		s.T().Fatal(err)
	}

	fmt.Println("Suite teardown completed.")
}

func TestVehicleControllerTestSuite(t *testing.T) {
	suite.Run(t, new(VehicleControllerTestSuite))
}

type deps struct {
	logger   zerolog.Logger
	identity service.IdentityAPI
}

func createMockDependencies(_ *testing.T) deps {
	logger := zerolog.New(os.Stdout).With().
		Timestamp().
		Str("app", "ingest-compass-iot").
		Logger()

	identity := mocks.NewIdentityAPIMock([]models.Vehicle{}, []models.DeviceDefinition{})

	return deps{
		logger:   logger,
		identity: identity,
	}
}

func (s *VehicleControllerTestSuite) TestGetVerificationStatusForVins() {
	t := s.T()
	mockDeps := createMockDependencies(t)

	c := NewVehiclesController(&config.Settings{Port: "3000"}, &mockDeps.logger, mockDeps.identity, s.vs, s.river, nil, nil)
	app := fiber.New(fiber.Config{
		EnableSplittingOnParsers: true,
	})
	app.Get("/vehicle/verify", test.AuthInjectorTestHandler("testUserID", nil), c.GetVerificationStatusForVins)

	s.Run("Get verification status for empty VIN list", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})

	s.Run("Get verification status for comma-separated list of empty VINs", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=,,,",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})

	s.Run("Get verification status for list of empty VINs", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins[]=&vins[]=",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})

	s.Run("Get verification status for list of invalid VINs", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=123,A345",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})

	s.Run("Get verification status for a single valid, but unknown VIN", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=ABCDEFG1234567811",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusOK, response.StatusCode)

		body, _ := io.ReadAll(response.Body)

		expected := StatusForVinsResponse{
			Statuses: []VinStatus{
				{
					Vin:     "ABCDEFG1234567811",
					Status:  "Unknown",
					Details: "Unknown",
				},
			},
		}

		expectedJSON, err := json.Marshal(expected)
		assert.NilError(t, err)

		assert.Equal(t, string(body), string(expectedJSON))
	})

	s.Run("Get verification status for a list of valid, but unknown VINs", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=ABCDEFG1234567811,ABCDEFG1234567812",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusOK, response.StatusCode)

		body, _ := io.ReadAll(response.Body)

		expected := StatusForVinsResponse{
			Statuses: []VinStatus{
				{
					Vin:     "ABCDEFG1234567811",
					Status:  "Unknown",
					Details: "Unknown",
				},
				{
					Vin:     "ABCDEFG1234567812",
					Status:  "Unknown",
					Details: "Unknown",
				},
			},
		}

		expectedJSON, err := json.Marshal(expected)
		assert.NilError(t, err)

		assert.Equal(t, string(body), string(expectedJSON))
	})

	s.Run("Fails for duplicates", func() {
		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=ABCDEFG1234567811,ABCDEFG1234567811",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})

	dbVin := dbmodels.Vin{
		Vin:              "ABCDEFG1234567812",
		OnboardingStatus: onboarding.OnboardingStatusMintSuccess,
	}

	s.Run("Get verification status for a list of valid VINs, some known, some not", func() {
		require.NoError(t, dbVin.Insert(s.ctx, s.pdb.DBS().Writer, boil.Infer()))

		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=ABCDEFG1234567811,ABCDEFG1234567812",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusOK, response.StatusCode)

		body, _ := io.ReadAll(response.Body)

		expected := StatusForVinsResponse{
			Statuses: []VinStatus{
				{
					Vin:     "ABCDEFG1234567811",
					Status:  "Unknown",
					Details: "Unknown",
				},
				{
					Vin:     "ABCDEFG1234567812",
					Status:  "Success",
					Details: "MintSuccess",
				},
			},
		}

		expectedJSON, err := json.Marshal(expected)
		assert.NilError(t, err)

		assert.Equal(t, string(body), string(expectedJSON))

		_, err = dbVin.Delete(s.ctx, s.pdb.DBS().Writer)
		assert.NilError(t, err)
	})

	s.Run("Does not return known VINs when they're not specified", func() {
		require.NoError(t, dbVin.Insert(s.ctx, s.pdb.DBS().Writer, boil.Infer()))

		req, _ := http.NewRequest(
			"GET",
			"/vehicle/verify?vins=ABCDEFG1234567811",
			strings.NewReader(""),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusOK, response.StatusCode)

		body, _ := io.ReadAll(response.Body)

		expected := StatusForVinsResponse{
			Statuses: []VinStatus{
				{
					Vin:     "ABCDEFG1234567811",
					Status:  "Unknown",
					Details: "Unknown",
				},
			},
		}

		expectedJSON, err := json.Marshal(expected)
		assert.NilError(t, err)

		assert.Equal(t, string(body), string(expectedJSON))

		_, err = dbVin.Delete(s.ctx, s.pdb.DBS().Writer)
		assert.NilError(t, err)
	})
}

func (s *VehicleControllerTestSuite) TestSubmitVerificationForVins_InvalidVins() {
	t := s.T()
	mockDeps := createMockDependencies(t)

	c := NewVehiclesController(&config.Settings{Port: "3000"}, &mockDeps.logger, mockDeps.identity, s.vs, s.river, nil, nil)
	app := fiber.New(fiber.Config{
		EnableSplittingOnParsers: true,
	})
	app.Post("/vehicle/verify", test.AuthInjectorTestHandler("testUserID", nil), c.SubmitVerificationForVins)

	s.Run("Submit empty VIN list", func() {
		payload := SubmitVinVerificationParams{
			Vins: make([]VinWithCountryCode, 0),
		}

		payloadJSON, err := json.Marshal(payload)
		assert.NilError(t, err)

		req := test.BuildRequest(
			"POST",
			"/vehicle/verify",
			string(payloadJSON),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusOK, response.StatusCode)

		body, _ := io.ReadAll(response.Body)

		expected := StatusForVinsResponse{
			Statuses: make([]VinStatus, 0),
		}

		expectedJSON, err := json.Marshal(expected)
		assert.NilError(t, err)

		assert.Equal(t, string(body), string(expectedJSON))
	})

	s.Run("Submit list of empty VINs", func() {
		payload := SubmitVinVerificationParams{
			Vins: []VinWithCountryCode{
				{Vin: "", CountryCode: "USA"},
				{Vin: "", CountryCode: ""},
				{Vin: "", CountryCode: "POL"},
			},
		}

		payloadJSON, err := json.Marshal(payload)
		assert.NilError(t, err)

		req := test.BuildRequest(
			"POST",
			"/vehicle/verify",
			string(payloadJSON),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})

	s.Run("Submit list with invalid VINs", func() {
		payload := SubmitVinVerificationParams{
			Vins: []VinWithCountryCode{
				{Vin: "123", CountryCode: "USA"},
				{Vin: "ABC", CountryCode: ""},
				{Vin: "FOOBAR321", CountryCode: "POL"},
			},
		}

		payloadJSON, err := json.Marshal(payload)
		assert.NilError(t, err)

		req := test.BuildRequest(
			"POST",
			"/vehicle/verify",
			string(payloadJSON),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusBadRequest, response.StatusCode)
	})
}

func (s *VehicleControllerTestSuite) TestSubmitVerificationForVins_ValidVins() {
	t := s.T()
	mockDeps := createMockDependencies(t)

	c := NewVehiclesController(&config.Settings{Port: "3000"}, &mockDeps.logger, mockDeps.identity, s.vs, s.river, nil, nil)
	app := fiber.New(fiber.Config{
		EnableSplittingOnParsers: true,
	})
	app.Post("/vehicle/verify", test.AuthInjectorTestHandler("testUserID", nil), c.SubmitVerificationForVins)

	s.Run("Get verification status for list of valid, unknown VINs", func() {
		payload := SubmitVinVerificationParams{
			Vins: []VinWithCountryCode{
				{Vin: "ABCDEFG1234567811", CountryCode: "USA"},
				{Vin: "ABCDEFG1234567812", CountryCode: "USA"},
				{Vin: "ABCDEFG1234567813", CountryCode: "POL"},
			},
		}

		payloadJSON, err := json.Marshal(payload)
		assert.NilError(t, err)

		req := test.BuildRequest(
			"POST",
			"/vehicle/verify",
			string(payloadJSON),
		)
		response, _ := app.Test(req)
		assert.Equal(t, fiber.StatusOK, response.StatusCode)

		body, _ := io.ReadAll(response.Body)

		expected := StatusForVinsResponse{
			Statuses: []VinStatus{
				{
					Vin:     "ABCDEFG1234567811",
					Status:  "Pending",
					Details: "VerificationSubmitPending",
				},
				{
					Vin:     "ABCDEFG1234567812",
					Status:  "Pending",
					Details: "VerificationSubmitPending",
				},
				{
					Vin:     "ABCDEFG1234567813",
					Status:  "Pending",
					Details: "VerificationSubmitPending",
				},
			},
		}

		expectedJSON, err := json.Marshal(expected)
		assert.NilError(t, err)

		assert.Equal(t, string(body), string(expectedJSON))
	})
}
