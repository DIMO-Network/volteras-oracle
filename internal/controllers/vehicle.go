package controllers

import (
	"database/sql"
	"fmt"
	"github.com/DIMO-Network/go-transactions"
	registry "github.com/DIMO-Network/go-transactions/contracts"
	"github.com/DIMO-Network/go-zerodev"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/DIMO-Network/volteras-oracle/internal/kafka"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	signer "github.com/ethereum/go-ethereum/signer/core/apitypes"
	"math/big"
	"strconv"

	"github.com/DIMO-Network/shared/pkg/logfields"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/onboarding"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
	"github.com/riverqueue/river"
	"github.com/rs/zerolog"
	"github.com/tidwall/gjson"
	"github.com/volatiletech/null/v8"
	"regexp"
	"slices"
	"strings"
)

var vinRegexp, _ = regexp.Compile("^[A-HJ-NPR-Z0-9]{17}$")

type VehicleController struct {
	settings    *config.Settings
	logger      *zerolog.Logger
	identity    service.IdentityAPI
	vs          *service.Vehicle
	riverClient *river.Client[pgx.Tx]
	ws          service.SDWalletsAPI
	tr          *transactions.Client
}

func NewVehiclesController(settings *config.Settings, logger *zerolog.Logger, identity service.IdentityAPI, vs *service.Vehicle, riverClient *river.Client[pgx.Tx], ws service.SDWalletsAPI, tr *transactions.Client) *VehicleController {
	return &VehicleController{
		settings:    settings,
		logger:      logger,
		identity:    identity,
		vs:          vs,
		riverClient: riverClient,
		ws:          ws,
		tr:          tr,
	}
}

func getWalletAddress(c *fiber.Ctx) (common.Address, error) {
	user := c.Locals("user").(*jwt.Token)
	claims := user.Claims.(jwt.MapClaims)
	address, ok := claims["ethereum_address"].(string)
	if !ok {
		return common.Address{}, errors.New("wallet_address not found in claims")
	}
	return common.HexToAddress(address), nil
}

// GetVehicles
// @Summary Get user's vehicles
// @Description Get user's vehicles from Identity API and add external ID (VIN)
// @Produce json
// @Success 200
// @Security     BearerAuth
// @Router /v1/vehicles [get]
func (v *VehicleController) GetVehicles(c *fiber.Ctx) error {
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	identityVehicles, err := (v.identity).FetchVehiclesByWalletAddress(walletAddress.String())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to load vehicles from Identity API",
		})
	}

	tokenIDsToCheck := []int64{}
	vehiclesByTokenID := make(map[int64]models.Vehicle)
	returnVehicles := []models.Vehicle{}

	for _, vehicle := range identityVehicles {
		tokenIDsToCheck = append(tokenIDsToCheck, vehicle.TokenID)
		vehiclesByTokenID[vehicle.TokenID] = vehicle
	}

	vins, err := v.vs.GetVinsByTokenIDs(c.Context(), tokenIDsToCheck)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to load vehicles from Vehicle Service",
		})
	}

	for _, vin := range vins {
		tid, tidErr := vin.VehicleTokenID.Value()
		if tidErr != nil {
			continue
		}

		vehicle, ok := vehiclesByTokenID[tid.(int64)]
		if !ok {
			continue
		}

		vehicle.VIN = vin.Vin
		vehicle.ConnectionStatus = vin.ConnectionStatus.String
		vehicle.DisconnectionStatus = vin.DisconnectionStatus.String
		returnVehicles = append(returnVehicles, vehicle)
	}

	return c.JSON(VehiclesResponse{
		Vehicles: returnVehicles,
	})
}

type VehiclesResponse struct {
	Vehicles []models.Vehicle `json:"vehicles"`
}

// GetVehicleByExternalID
// @Summary Get user's vehicle by external ID
// @Description Get user's vehicle by external ID (VIN)
// @Produce json
// @Success 200
// @Security     BearerAuth
// @Router /v1/vehicle/{externalID} [get]
func (v *VehicleController) GetVehicleByExternalID(c *fiber.Ctx) error {
	externalID := c.Params("externalID")
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	identityVehicles, err := (v.identity).FetchVehiclesByWalletAddress(walletAddress.String())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to load vehicles from Identity API",
		})
	}

	vin, err := v.vs.GetVehicleByExternalID(c.Context(), externalID)
	if err != nil {
		if errors.Is(err, service.ErrVehicleNotFound) {
			return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicle")
		}

		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to load vehicle from Database",
		})
	}

	vehiclesByTokenID := make(map[int64]models.Vehicle)
	for _, vehicle := range identityVehicles {
		vehiclesByTokenID[vehicle.TokenID] = vehicle
	}

	tid, tidErr := vin.VehicleTokenID.Value()
	if tidErr != nil {
		return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicle")
	}

	vehicle, ok := vehiclesByTokenID[tid.(int64)]
	if !ok {
		return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicle")
	}

	vehicle.VIN = vin.Vin

	return c.JSON(VehicleResponse{
		Vehicle: vehicle,
	})
}

type VehicleResponse struct {
	Vehicle models.Vehicle `json:"vehicle"`
}

// RegisterVehicle
// @Summary Checks and registers existing vehicle in internal oracle mapping DB
// @Description Checks and registers existing vehicle in internal oracle mapping DB
// @Produce json
// @Success 200
// @Security     BearerAuth
// @Router /v1/vehicle/register [post]
func (v *VehicleController) RegisterVehicle(c *fiber.Ctx) error {
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	vinToRegister := gjson.GetBytes(c.Body(), "vin")
	tokenIDToRegister := gjson.GetBytes(c.Body(), "token_id")

	if !vinToRegister.Exists() || len(vinToRegister.String()) != 17 || !tokenIDToRegister.Exists() {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Missing or invalid VIN or Token ID",
		})
	}

	// Check if the vehicle is available in identity-api
	identityVehicle, err := (v.identity).FetchVehicleByTokenID(tokenIDToRegister.Int())
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to load vehicle from Identity API",
		})
	}

	// we may not find vehicle (graphql still returns 200 and unmarshal creates empty objects)
	if identityVehicle.TokenID == 0 {
		return c.Status(fiber.StatusNotFound).JSON(fiber.Map{
			"error": "Vehicle not found",
		})
	}

	// vehicle can't be owned by someone else
	if identityVehicle.Owner != walletAddress.String() {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "Vehicle not owned by wallet",
		})
	}

	var newVin dbmodels.Vin
	newVin.OnboardingStatus = onboarding.OnboardingStatusSubmitUnknown
	newVin.Vin = vinToRegister.String()
	newVin.VehicleTokenID = null.Int64From(tokenIDToRegister.Int())

	// check if this VIN is already registered
	vin, err := v.vs.GetVehicleByExternalID(c.Context(), vinToRegister.String())

	if err != nil {
		// if now found, we're still good, so fail only on other errors
		if !errors.Is(err, service.ErrVehicleNotFound) {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicle from Database",
			})
		}
	}

	if vin != nil {
		// If registered VIN has TokenID, but it's different, that's bad
		if !vin.VehicleTokenID.IsZero() && vin.VehicleTokenID.Int64 != tokenIDToRegister.Int() {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{
				"error": "Vehicle VIN assigned to another TokenID",
			})
		}

		newVin = *vin
	}

	if identityVehicle.SyntheticDevice.TokenID != 0 {
		newVin.SyntheticTokenID = null.Int64From(identityVehicle.SyntheticDevice.TokenID)
	}

	if identityVehicle.Definition.ID != "" {
		newVin.DeviceDefinitionID = null.StringFrom(identityVehicle.Definition.ID)
	}

	// We allow to either insert new row or update Synthetic TokenID for existing row
	err = v.vs.InsertOrUpdateVin(c.Context(), &newVin)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to register Vehicle",
		})
	}

	identityVehicle.VIN = vinToRegister.String()

	return c.JSON(VehicleResponse{
		Vehicle: *identityVehicle,
	})
}

type VehicleRegisterPayload struct {
	Vin     string `json:"vin"`
	TokenID string `json:"token_id"`
}

type VinsGetParams struct {
	Vins []string `json:"vins" query:"vins"`
}

// GetVerificationStatusForVins
// @Summary Get verification status for each of the submitted VINs
// @Produce json
// @Success 200
// @Security     BearerAuth
// @Router /v1/vehicle/verify [get]
func (v *VehicleController) GetVerificationStatusForVins(c *fiber.Ctx) error {
	params := new(VinsGetParams)
	if err := c.QueryParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}

	localLog := v.logger.With().Interface("vins", params.Vins).Str(logfields.FunctionName, "GetVerificationStatusForVins").Logger()
	localLog.Debug().Interface("vins", params.Vins).Msg("Checking Verification Status for Vins")

	validVins := make([]string, 0, len(params.Vins))
	for _, vin := range params.Vins {
		strippedVin := strings.TrimSpace(vin)
		if v.isValidVin(strippedVin) {
			validVins = append(validVins, strippedVin)
		}
	}

	if len(validVins) != len(params.Vins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid VINs provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs", len(validVins))

	statuses := make([]VinStatus, 0, len(validVins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVins(c.Context(), validVins)
		if err != nil {
			if errors.Is(err, service.ErrVehicleNotFound) {
				return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		indexedVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedVins[vin.Vin] = vin
		}

		for _, vin := range validVins {
			dbVin, ok := indexedVins[vin]
			if !ok {
				statuses = append(statuses, VinStatus{
					Vin:     vin,
					Status:  "Unknown",
					Details: "Unknown",
				})
			} else {
				statuses = append(statuses, VinStatus{
					Vin:     vin,
					Status:  onboarding.GetVerificationStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}

type VinWithCountryCode struct {
	Vin         string `json:"vin"`
	CountryCode string `json:"countryCode"`
}
type SubmitVinVerificationParams struct {
	Vins []VinWithCountryCode `json:"vins"`
}

func (v *VehicleController) canSubmitVerificationJob(record *dbmodels.Vin) bool {
	if record == nil {
		return false
	}

	verified := onboarding.IsVerified(record.OnboardingStatus)
	failed := onboarding.IsFailure(record.OnboardingStatus)
	pending := onboarding.IsPending(record.OnboardingStatus)

	return !verified && (failed || !pending)
}

func (v *VehicleController) isValidVin(vin string) bool {
	if v.settings.EnableVendorTestMode {
		return len(vin) == 17
	}

	return vinRegexp.MatchString(vin)
}

// SubmitVerificationForVins
// @Summary Submits VINs with country codes for verification
// @Description Decodes the VINs to Device Definitions and validates vendor connectivity
// @Produce json
// @Success 200
// @Security BearerAuth
// @Router /v1/vehicle/verify [post]
func (v *VehicleController) SubmitVerificationForVins(c *fiber.Ctx) error {
	params := new(SubmitVinVerificationParams)
	if err := c.BodyParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}
	localLog := v.logger.With().Interface("vins", params.Vins).Str(logfields.FunctionName, "SubmitVerificationForVins").Logger()
	localLog.Debug().Interface("vins", params.Vins).Msg("Submitting Verification for VINs")

	validVins := make([]string, 0, len(params.Vins))
	validVinsWithCountryCode := make([]VinWithCountryCode, 0, len(params.Vins))
	for _, paramVin := range params.Vins {
		strippedVin := strings.TrimSpace(paramVin.Vin)
		strippedCountryCode := strings.TrimSpace(paramVin.CountryCode)
		if v.isValidVin(strippedVin) {
			validVins = append(validVins, strippedVin)
			validVinsWithCountryCode = append(validVinsWithCountryCode, VinWithCountryCode{Vin: strippedVin, CountryCode: strippedCountryCode})
		}
	}

	if len(validVins) != len(params.Vins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid VINs provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs", len(validVins))

	vinStatuses := make([]VinStatus, 0, len(params.Vins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVins(c.Context(), validVins)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		indexedDbVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedDbVins[vin.Vin] = vin
		}

		for _, vin := range validVinsWithCountryCode {
			dbVin, ok := indexedDbVins[vin.Vin]
			if !ok {
				dbVin = &dbmodels.Vin{
					Vin:              vin.Vin,
					OnboardingStatus: onboarding.OnboardingStatusSubmitUnknown,
				}
			}

			if v.canSubmitVerificationJob(dbVin) {
				localLog.Debug().Str(logfields.VIN, vin.Vin).Str(logfields.CountryCode, vin.CountryCode).Msg("Submitting VIN verification job")
				_, err = v.riverClient.Insert(c.Context(), onboarding.VerifyArgs{
					VIN:         vin.Vin,
					CountryCode: vin.CountryCode,
				}, nil)

				if err != nil {
					v.logger.Error().Str(logfields.VIN, vin.Vin).Str(logfields.CountryCode, vin.CountryCode).Err(err).Msg("Failed to submit VIN verification job")
					vinStatuses = append(vinStatuses, VinStatus{
						Vin:     vin.Vin,
						Status:  "Failure",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusSubmitFailure),
					})
				} else {
					v.logger.Debug().Str(logfields.VIN, vin.Vin).Str(logfields.CountryCode, vin.CountryCode).Msg("VIN verification job submitted")
					vinStatuses = append(vinStatuses, VinStatus{
						Vin:     vin.Vin,
						Status:  "Pending",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusSubmitPending),
					})
				}
			} else {
				v.logger.Debug().Str(logfields.VIN, vin.Vin).Str(logfields.CountryCode, vin.CountryCode).Msg("Skipping VIN verification job submission")
				vinStatuses = append(vinStatuses, VinStatus{
					Vin:     vin.Vin,
					Status:  onboarding.GetVerificationStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}

			err = v.vs.InsertOrUpdateVin(c.Context(), dbVin)

			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": fmt.Sprintf("Failed to submit verification for vin: %s", vin),
				})
			}
			localLog.Debug().Str(logfields.VIN, vin.Vin).Str(logfields.CountryCode, vin.CountryCode).Msg("Submitted Verification for VIN")
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: vinStatuses,
	})
}

type VinStatus struct {
	Vin     string `json:"vin"`
	Status  string `json:"status"`
	Details string `json:"details"`
}

type StatusForVinsResponse struct {
	Statuses []VinStatus `json:"statuses"`
}

func (v *VehicleController) GetMintDataForVins(c *fiber.Ctx) error {
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	params := new(VinsGetParams)
	if err := c.QueryParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}
	localLog := v.logger.With().Interface("vins", params.Vins).Str(logfields.FunctionName, "GetMintDataForVins").Logger()
	localLog.Debug().Msg("Checking Verification Status for Vins")

	validVins := make([]string, 0, len(params.Vins))
	for _, vin := range params.Vins {
		strippedVin := strings.TrimSpace(vin)
		if v.isValidVin(strippedVin) {
			validVins = append(validVins, strippedVin)
		}
	}

	if len(validVins) != len(params.Vins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid VINs provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs for get mint", len(validVins))

	mintingData := make([]VinTransactionData, 0, len(validVins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVinsAndOnboardingStatusRange(
			c.Context(),
			validVins,
			onboarding.OnboardingStatusVendorValidationSuccess,
			onboarding.OnboardingStatusMintFailure,
			[]int{onboarding.OnboardingStatusBurnSDSuccess, onboarding.OnboardingStatusBurnVehicleSuccess},
		)
		if err != nil {
			if errors.Is(err, service.ErrVehicleNotFound) {
				return fiber.NewError(fiber.StatusBadRequest, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		mintedVins, err := v.vs.GetVehiclesByVinsAndOnboardingStatus(
			c.Context(),
			validVins,
			onboarding.OnboardingStatusMintSuccess,
		)
		if err != nil {
			if !errors.Is(err, service.ErrVehicleNotFound) {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to load vehicles from Database",
				})
			}
		}

		vendorFailedMintedVins := make(dbmodels.VinSlice, 0, len(mintedVins))
		for _, vin := range mintedVins {
			if vin.ConnectionStatus.String == "failed" {
				vendorFailedMintedVins = append(vendorFailedMintedVins, vin)
			}
		}

		dbVins = append(dbVins, vendorFailedMintedVins...)

		if len(dbVins) != len(validVins) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Some of the VINs are not verified or already onboarded",
			})
		}

		indexedVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedVins[vin.Vin] = vin
		}

		for _, dbVin := range dbVins {
			localLog.Debug().Str(logfields.DefinitionID, dbVin.DeviceDefinitionID.String).Msgf("getting definition for vin")
			definition, err := v.identity.GetDeviceDefinitionByID(dbVin.DeviceDefinitionID.String)
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to load device definition",
				})
			}

			var typedData *signer.TypedData

			if dbVin.VehicleTokenID.IsZero() {
				typedData = v.tr.GetMintVehicleWithDDTypedData(
					new(big.Int).SetUint64(definition.Manufacturer.TokenID),
					walletAddress,
					definition.DeviceDefinitionID,
					[]registry.AttributeInfoPair{
						{
							Attribute: "Make",
							Info:      definition.Manufacturer.Name,
						},
						{
							Attribute: "Model",
							Info:      definition.Model,
						},
						{
							Attribute: "Year",
							Info:      strconv.Itoa(definition.Year),
						},
					},
				)
			} else if dbVin.SyntheticTokenID.IsZero() {

				var integrationOrConnectionID *big.Int
				ok := false

				if v.settings.EnableMintingWithConnectionTokenID {
					integrationOrConnectionID, ok = new(big.Int).SetString(v.settings.ConnectionTokenID, 10)
					typedData = v.tr.GetMintSDTypedDataV2(integrationOrConnectionID, big.NewInt(dbVin.VehicleTokenID.Int64))
				} else {
					integrationOrConnectionID, ok = new(big.Int).SetString(v.settings.IntegrationTokenID, 10)
					typedData = v.tr.GetMintSDTypedData(integrationOrConnectionID, big.NewInt(dbVin.VehicleTokenID.Int64))
				}

				if !ok {
					v.logger.Error().Err(err).Msg("Failed to set integration or connection token ID")
					return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
						"error": "Failed to set integration or connection token ID",
					})
				}

			} else {
				if dbVin.ConnectionStatus.String != kafka.OperationStatusFailed {
					return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
						"error": "VIN already fully minted and connected or connection in progress",
					})
				}
			}

			vinMintingData := VinTransactionData{
				Vin: dbVin.Vin,
			}

			if typedData != nil {
				vinMintingData.TypedData = typedData
			}

			mintingData = append(mintingData, vinMintingData)
		}
	}

	return c.JSON(MintDataForVins{
		VinMintingData: mintingData,
	})
}

type SacdInput struct {
	Grantee     common.Address
	Permissions int64
	Expiration  int64
	Source      string
}

type VinTransactionData struct {
	Vin       string            `json:"vin"`
	TypedData *signer.TypedData `json:"typedData,omitempty"`
	Signature hexutil.Bytes     `json:"signature,omitempty"`
}

type MintDataForVins struct {
	VinMintingData []VinTransactionData `json:"vinMintingData"`
	Sacd           SacdInput            `json:"sacd,omitempty"`
}

type VinUserOperationData struct {
	Vin           string                 `json:"vin"`
	UserOperation *zerodev.UserOperation `json:"userOperation"`
	Hash          common.Hash            `json:"hash"`
	Signature     hexutil.Bytes          `json:"signature,omitempty"`
}

type DisconnectDataForVins struct {
	VinDisconnectData []VinUserOperationData `json:"vinDisconnectData"`
}

func (v *VehicleController) SubmitMintDataForVins(c *fiber.Ctx) error {
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	params := new(MintDataForVins)
	if err := c.BodyParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse minting data",
		})
	}

	localLog := v.logger.With().Str(logfields.FunctionName, "SubmitMintDataForVins").Logger()
	localLog.Debug().Msg("Submitting VINs to mint")

	validVins := make([]string, 0, len(params.VinMintingData))
	validVinsMintingData := make([]VinTransactionData, 0, len(params.VinMintingData))
	for _, paramVin := range params.VinMintingData {
		validatedVinMintingData, err := v.getValidatedMintingData(&paramVin, walletAddress)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid minting data",
			})
		}

		validVins = append(validVins, validatedVinMintingData.Vin)
		validVinsMintingData = append(validVinsMintingData, *validatedVinMintingData)
	}

	if len(validVins) != len(params.VinMintingData) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid minting data provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs", len(validVins))

	statuses := make([]VinStatus, 0, len(params.VinMintingData))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVins(c.Context(), validVins)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		mintedVins, err := v.vs.GetVehiclesByVinsAndOnboardingStatus(
			c.Context(),
			validVins,
			onboarding.OnboardingStatusMintSuccess,
		)
		if err != nil {
			if !errors.Is(err, service.ErrVehicleNotFound) {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to load vehicles from Database",
				})
			}
		}

		vendorFailedMintedVins := make(dbmodels.VinSlice, 0, len(mintedVins))
		for _, vin := range mintedVins {
			if vin.ConnectionStatus.String == "failed" {
				vendorFailedMintedVins = append(vendorFailedMintedVins, vin)
			}
		}

		dbVins = append(dbVins, vendorFailedMintedVins...)

		indexedDbVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedDbVins[vin.Vin] = vin
		}

		for _, mint := range validVinsMintingData {
			dbVin, ok := indexedDbVins[mint.Vin]
			if !ok {
				dbVin = &dbmodels.Vin{
					Vin:              mint.Vin,
					OnboardingStatus: onboarding.OnboardingStatusMintSubmitUnknown,
				}
			}

			var sacd *onboarding.OnboardingSacd

			if params.Sacd.Expiration != 0 && params.Sacd.Permissions != 0 {
				sacd = &onboarding.OnboardingSacd{
					Grantee:     params.Sacd.Grantee,
					Expiration:  new(big.Int).SetInt64(params.Sacd.Expiration),
					Permissions: new(big.Int).SetInt64(params.Sacd.Permissions),
					Source:      params.Sacd.Source,
				}
			}

			if v.canSubmitMintingJob(dbVin) {
				localLog.Debug().Str(logfields.VIN, mint.Vin).Msg("Submitting minting job")
				_, err = v.riverClient.Insert(c.Context(), onboarding.OnboardingArgs{
					VIN:       mint.Vin,
					TypedData: mint.TypedData,
					Signature: mint.Signature,
					Owner:     walletAddress,
					Sacd:      sacd,
				}, nil)

				if err != nil {
					v.logger.Error().Str(logfields.VIN, mint.Vin).Err(err).Msg("Failed to submit minting job")
					statuses = append(statuses, VinStatus{
						Vin:     mint.Vin,
						Status:  "Failure",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusMintSubmitFailure),
					})
				} else {
					v.logger.Debug().Str(logfields.VIN, mint.Vin).Msg("minting job submitted")
					statuses = append(statuses, VinStatus{
						Vin:     mint.Vin,
						Status:  "Pending",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusMintSubmitPending),
					})
				}
			} else {
				v.logger.Debug().Str(logfields.VIN, mint.Vin).Msg("Skipping minting job submission")
				statuses = append(statuses, VinStatus{
					Vin:     mint.Vin,
					Status:  onboarding.GetVerificationStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}

			err = v.vs.InsertOrUpdateVin(c.Context(), dbVin)

			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": fmt.Sprintf("Failed to submit verification for mint: %v", mint),
				})
			}
			localLog.Debug().Str(logfields.VIN, mint.Vin).Msg("Submitted mint for VIN")
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}

func (v *VehicleController) getValidatedMintingData(data *VinTransactionData, _ common.Address) (*VinTransactionData, error) {
	result := new(VinTransactionData)

	// Validate VIN
	strippedVin := strings.TrimSpace(data.Vin)
	if !v.isValidVin(strippedVin) {
		return nil, errors.New("invalid VIN")
	}

	// Signature validation would require call to a wallet contract
	//// Validate signature
	//hash, _, err := signer.TypedDataAndHash(data.TypedData)
	//if err != nil {
	//	return nil, err
	//}
	//
	//signature := make([]byte, 65)
	//copy(signature, data.Signature[21:])
	//signature[64] -= 27
	//pubKey, err := crypto.SigToPub(hash, signature)
	//if err != nil {
	//	return nil, err
	//}

	//derivedAddress := crypto.PubkeyToAddress(*pubKey)
	//if derivedAddress != owner {
	//	return nil, errors.New("invalid signature")
	//}

	// Validate typed data with device definition (if applicable)
	if data.TypedData != nil && data.TypedData.PrimaryType == "MintVehicleWithDeviceDefinitionSign" {
		_, err := v.identity.GetDeviceDefinitionByID(data.TypedData.Message["deviceDefinitionId"].(string))
		if err != nil {
			return nil, err
		}

		// TODO: validate if definition aligns with message data
	}

	result.Vin = strippedVin
	result.TypedData = data.TypedData
	result.Signature = data.Signature
	return result, nil
}

func (v *VehicleController) canSubmitMintingJob(record *dbmodels.Vin) bool {
	if record == nil {
		return false
	}

	minted := onboarding.IsMinted(record.OnboardingStatus)
	burned := onboarding.IsDisconnected(record.OnboardingStatus)
	failed := onboarding.IsFailure(record.OnboardingStatus)
	failedConnection := record.ConnectionStatus.String == "failed"
	pending := onboarding.IsMintPending(record.OnboardingStatus) || onboarding.IsDisconnectPending(record.OnboardingStatus)

	return (minted && failedConnection) || (!minted || burned) && (failed || !pending)
}

func (v *VehicleController) GetMintStatusForVins(c *fiber.Ctx) error {
	params := new(VinsGetParams)
	if err := c.QueryParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}

	localLog := v.logger.With().Str(logfields.FunctionName, "GetMintStatusForVins").Interface("validVins", params.Vins).Logger()
	localLog.Debug().Interface("vins", params.Vins).Msg("Checking Verification Status for Vins")

	validVins := make([]string, 0, len(params.Vins))
	for _, vin := range params.Vins {
		strippedVin := strings.TrimSpace(vin)
		if v.isValidVin(strippedVin) {
			validVins = append(validVins, strippedVin)
		}
	}

	if len(validVins) != len(params.Vins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid VINs provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs", len(validVins))

	statuses := make([]VinStatus, 0, len(validVins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVins(c.Context(), validVins)
		if err != nil {
			if errors.Is(err, service.ErrVehicleNotFound) {
				return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		indexedVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedVins[vin.Vin] = vin
		}

		for _, vin := range validVins {
			dbVin, ok := indexedVins[vin]
			if !ok {
				statuses = append(statuses, VinStatus{
					Vin:     vin,
					Status:  "Unknown",
					Details: "Unknown",
				})
			} else {
				statuses = append(statuses, VinStatus{
					Vin:     vin,
					Status:  onboarding.GetMintStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}

// GetDisconnectDataForVins
// @Summary Get verification status for each of the submitted VINs
// @Produce json
// @Success 200
// @Security     BearerAuth
// @Router /v1/vehicle/disconnect [get]
func (v *VehicleController) GetDisconnectDataForVins(c *fiber.Ctx) error {
	params := new(VinsGetParams)
	if err := c.QueryParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}

	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	localLog := v.logger.With().Interface("vins", params.Vins).Str(logfields.FunctionName, "GetDisconnectDataForVins").Logger()
	localLog.Debug().Msg("Getting disconnection data for Vins")

	validVins := make([]string, 0, len(params.Vins))
	for _, vin := range params.Vins {
		strippedVin := strings.TrimSpace(vin)
		if v.isValidVin(strippedVin) {
			validVins = append(validVins, strippedVin)
		}
	}

	if len(validVins) != len(params.Vins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid VINs provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs for disconnection", len(validVins))

	disconnectionData := make([]VinUserOperationData, 0, len(validVins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVinsAndOnboardingStatusRange(c.Context(), validVins, onboarding.OnboardingStatusMintSuccess, onboarding.OnboardingStatusBurnSDFailure, nil)
		if err != nil {
			if errors.Is(err, service.ErrVehicleNotFound) {
				return fiber.NewError(fiber.StatusBadRequest, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		if len(dbVins) != len(validVins) {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Some of the VINs are not fully onboarded",
			})
		}

		indexedVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedVins[vin.Vin] = vin
		}

		identityVehicles, err := v.identity.FetchVehiclesByWalletAddress(walletAddress.String())
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to fetch identity vehicles",
			})
		}

		indexedIdentityVehicles := make(map[int64]models.Vehicle)
		for _, identityVehicle := range identityVehicles {
			indexedIdentityVehicles[identityVehicle.TokenID] = identityVehicle
		}

		for _, dbVin := range dbVins {
			identityVehicle, ok := indexedIdentityVehicles[dbVin.VehicleTokenID.Int64]
			if !ok {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "VIN not owned",
				})
			}

			fullyConnected := !dbVin.VehicleTokenID.IsZero() && !dbVin.SyntheticTokenID.IsZero()

			if !fullyConnected {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "VIN not minted",
				})
			}

			fullyConnectedIdentity := identityVehicle.TokenID == dbVin.VehicleTokenID.Int64 && identityVehicle.SyntheticDevice.TokenID == dbVin.SyntheticTokenID.Int64

			if !fullyConnectedIdentity {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "TokenIDs mismatch",
				})
			}

			op, hash, err := v.tr.GetBurnSDByOwnerUserOperationAndHash(walletAddress, big.NewInt(dbVin.SyntheticTokenID.Int64))
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to get Burn SD operation data",
				})
			}

			vinMintingData := VinUserOperationData{
				Vin:           dbVin.Vin,
				UserOperation: op,
				Hash:          *hash,
			}

			disconnectionData = append(disconnectionData, vinMintingData)
		}
	}

	return c.JSON(DisconnectDataForVins{
		VinDisconnectData: disconnectionData,
	})
}

func (v *VehicleController) SubmitDisconnectDataForVins(c *fiber.Ctx) error {
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	params := new(DisconnectDataForVins)
	if err := c.BodyParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse disconnection data",
		})
	}

	localLog := v.logger.With().Str(logfields.FunctionName, "SubmitDisconnectDataForVins").Logger()
	localLog.Debug().Msg("Submitting VINs to disconnect")

	validVins := make([]string, 0, len(params.VinDisconnectData))
	validVinsDisconnectData := make([]VinUserOperationData, 0, len(params.VinDisconnectData))
	for _, paramVin := range params.VinDisconnectData {
		validatedVinMintingData, err := v.getValidatedUserOperationData(&paramVin, walletAddress)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid disconnect data",
			})
		}

		validVins = append(validVins, validatedVinMintingData.Vin)
		validVinsDisconnectData = append(validVinsDisconnectData, *validatedVinMintingData)
	}

	if len(validVins) != len(params.VinDisconnectData) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid disconnect data provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs submitted to disconnect", len(validVins))

	statuses := make([]VinStatus, 0, len(params.VinDisconnectData))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVins(c.Context(), validVins)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		indexedDbVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedDbVins[vin.Vin] = vin
		}

		for _, disconnect := range validVinsDisconnectData {
			dbVin, ok := indexedDbVins[disconnect.Vin]
			if !ok {
				dbVin = &dbmodels.Vin{
					Vin:              disconnect.Vin,
					OnboardingStatus: onboarding.OnboardingStatusDisconnectSubmitUnknown,
				}
			}

			if v.canSubmitDisconnectJob(dbVin) {
				localLog.Debug().Str(logfields.VIN, disconnect.Vin).Msg("Submitting disconnect job")

				op := disconnect.UserOperation
				op.Signature = disconnect.Signature

				_, err = v.riverClient.Insert(c.Context(), onboarding.DisconnectArgs{
					VIN:           disconnect.Vin,
					UserOperation: disconnect.UserOperation,
				}, nil)

				if err != nil {
					v.logger.Error().Str(logfields.VIN, disconnect.Vin).Err(err).Msg("Failed to submit disconnect job")
					statuses = append(statuses, VinStatus{
						Vin:     disconnect.Vin,
						Status:  "Failure",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusDisconnectSubmitFailure),
					})
				} else {
					v.logger.Debug().Str(logfields.VIN, disconnect.Vin).Msg("disconnect job submitted")
					statuses = append(statuses, VinStatus{
						Vin:     disconnect.Vin,
						Status:  "Pending",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusDisconnectSubmitPending),
					})
				}
			} else {
				v.logger.Debug().Str(logfields.VIN, disconnect.Vin).Msg("Skipping disconnect job submission")
				statuses = append(statuses, VinStatus{
					Vin:     disconnect.Vin,
					Status:  onboarding.GetVerificationStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}

			err = v.vs.InsertOrUpdateVin(c.Context(), dbVin)

			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": fmt.Sprintf("Failed to submit disconnect: %v", disconnect),
				})
			}
			localLog.Debug().Str(logfields.VIN, disconnect.Vin).Msg("Submitted disconnect for VIN")
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}

func (v *VehicleController) getValidatedUserOperationData(data *VinUserOperationData, _ common.Address) (*VinUserOperationData, error) {
	result := new(VinUserOperationData)

	// Validate VIN
	strippedVin := strings.TrimSpace(data.Vin)
	if !v.isValidVin(strippedVin) {
		return nil, errors.New("invalid VIN")
	}

	result.Vin = strippedVin
	result.UserOperation = data.UserOperation
	result.Hash = data.Hash
	result.Signature = data.Signature
	return result, nil
}

func (v *VehicleController) canSubmitDisconnectJob(record *dbmodels.Vin) bool {
	if record == nil {
		return false
	}

	minted := onboarding.IsMinted(record.OnboardingStatus)
	failed := onboarding.IsDisconnectFailed(record.OnboardingStatus)
	pending := onboarding.IsDisconnectPending(record.OnboardingStatus)

	return (minted || failed) && !pending
}

func (v *VehicleController) GetDisconnectStatusForVins(c *fiber.Ctx) error {
	params := new(VinsGetParams)
	if err := c.QueryParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}

	localLog := v.logger.With().Str(logfields.FunctionName, "GetDisconnectStatusForVins").Interface("validVins", params.Vins).Logger()
	localLog.Debug().Interface("vins", params.Vins).Msg("Checking Disconnect Status for Vins")

	validVins := make([]string, 0, len(params.Vins))
	for _, vin := range params.Vins {
		strippedVin := strings.TrimSpace(vin)
		if v.isValidVin(strippedVin) {
			validVins = append(validVins, strippedVin)
		}
	}

	if len(validVins) != len(params.Vins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid VINs provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs", len(validVins))

	statuses := make([]VinStatus, 0, len(validVins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVins(c.Context(), validVins)
		if err != nil {
			if errors.Is(err, service.ErrVehicleNotFound) {
				return fiber.NewError(fiber.StatusNotFound, "Could not find Vehicles")
			}

			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": "Failed to load vehicles from Database",
			})
		}

		indexedVins := make(map[string]*dbmodels.Vin)
		for _, vin := range dbVins {
			indexedVins[vin.Vin] = vin
		}

		for _, vin := range validVins {
			dbVin, ok := indexedVins[vin]
			if !ok {
				statuses = append(statuses, VinStatus{
					Vin:     vin,
					Status:  "Unknown",
					Details: "Unknown",
				})
			} else {
				statuses = append(statuses, VinStatus{
					Vin:     vin,
					Status:  onboarding.GetDisconnectStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}
