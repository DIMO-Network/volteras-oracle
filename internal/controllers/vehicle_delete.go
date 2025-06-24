package controllers

import (
	"database/sql"
	"fmt"
	"github.com/DIMO-Network/shared/pkg/logfields"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/onboarding"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/friendsofgo/errors"
	"github.com/gofiber/fiber/v2"
	"math/big"
	"slices"
	"strings"
)

type DeleteDataForVins struct {
	VinDeleteData []VinUserOperationData `json:"vinDeleteData"`
}

func (v *VehicleController) GetDeleteDataForVins(c *fiber.Ctx) error {
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

	localLog := v.logger.With().Interface("vins", params.Vins).Str(logfields.FunctionName, "GetDeleteDataForVins").Logger()
	localLog.Debug().Msg("Getting deletion data for Vins")

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

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs for deletion", len(validVins))

	deletionData := make([]VinUserOperationData, 0, len(validVins))

	if len(validVins) > 0 {
		dbVins, err := v.vs.GetVehiclesByVinsAndOnboardingStatusRange(c.Context(), validVins, onboarding.OnboardingStatusBurnSDSuccess, onboarding.OnboardingStatusBurnVehicleFailure, nil)
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
				"error": "Some of the VINs are not disconnected",
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

			burnable := !dbVin.VehicleTokenID.IsZero() && dbVin.SyntheticTokenID.IsZero()

			if !burnable {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "VIN cannot be burned",
				})
			}

			burnableIdentity := identityVehicle.TokenID == dbVin.VehicleTokenID.Int64 && identityVehicle.SyntheticDevice.TokenID == 0

			if !burnableIdentity {
				return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
					"error": "TokenIDs mismatch",
				})
			}

			op, hash, err := v.tr.GetBurnVehicleByOwnerUserOperationAndHash(walletAddress, big.NewInt(dbVin.VehicleTokenID.Int64))
			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": "Failed to get Burn Vehicle operation data",
				})
			}

			vinMintingData := VinUserOperationData{
				Vin:           dbVin.Vin,
				UserOperation: op,
				Hash:          *hash,
			}

			deletionData = append(deletionData, vinMintingData)
		}
	}

	return c.JSON(DeleteDataForVins{
		VinDeleteData: deletionData,
	})
}

func (v *VehicleController) SubmitDeleteDataForVins(c *fiber.Ctx) error {
	walletAddress, err := getWalletAddress(c)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": "Failed to get wallet address",
		})
	}

	params := new(DeleteDataForVins)
	if err := c.BodyParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse delete data",
		})
	}

	localLog := v.logger.With().Str(logfields.FunctionName, "SubmitDeleteDataForVins").Logger()
	localLog.Debug().Msg("Submitting VINs to delete")

	validVins := make([]string, 0, len(params.VinDeleteData))
	validVinsDeleteData := make([]VinUserOperationData, 0, len(params.VinDeleteData))
	for _, paramVin := range params.VinDeleteData {
		validatedVinDeleteData, err := v.getValidatedUserOperationData(&paramVin, walletAddress)
		if err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid delete data",
			})
		}

		validVins = append(validVins, validatedVinDeleteData.Vin)
		validVinsDeleteData = append(validVinsDeleteData, *validatedVinDeleteData)
	}

	if len(validVins) != len(params.VinDeleteData) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid delete data provided",
		})
	}

	compactedVins := slices.Compact(validVins)
	if len(validVins) != len(compactedVins) {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Duplicated VINs",
		})
	}

	localLog.Debug().Interface("validVins", validVins).Msgf("Got %d valid VINs submitted to delete", len(validVins))

	statuses := make([]VinStatus, 0, len(params.VinDeleteData))

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

		for _, deleteVehicle := range validVinsDeleteData {
			dbVin, ok := indexedDbVins[deleteVehicle.Vin]
			if !ok {
				dbVin = &dbmodels.Vin{
					Vin:              deleteVehicle.Vin,
					OnboardingStatus: onboarding.OnboardingStatusDeleteSubmitUnknown,
				}
			}

			if v.canSubmitDeleteJob(dbVin) {
				localLog.Debug().Str(logfields.VIN, deleteVehicle.Vin).Msg("Submitting deleteVehicle job")

				op := deleteVehicle.UserOperation
				op.Signature = deleteVehicle.Signature

				_, err = v.riverClient.Insert(c.Context(), onboarding.DeleteArgs{
					VIN:           deleteVehicle.Vin,
					UserOperation: deleteVehicle.UserOperation,
				}, nil)

				if err != nil {
					v.logger.Error().Str(logfields.VIN, deleteVehicle.Vin).Err(err).Msg("Failed to submit deleteVehicle job")
					statuses = append(statuses, VinStatus{
						Vin:     deleteVehicle.Vin,
						Status:  "Failure",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusDeleteSubmitFailure),
					})
				} else {
					v.logger.Debug().Str(logfields.VIN, deleteVehicle.Vin).Msg("deleteVehicle job submitted")
					statuses = append(statuses, VinStatus{
						Vin:     deleteVehicle.Vin,
						Status:  "Pending",
						Details: onboarding.GetDetailedStatus(onboarding.OnboardingStatusDeleteSubmitPending),
					})
				}
			} else {
				v.logger.Debug().Str(logfields.VIN, deleteVehicle.Vin).Msg("Skipping deleteVehicle job submission")
				statuses = append(statuses, VinStatus{
					Vin:     deleteVehicle.Vin,
					Status:  onboarding.GetBurnStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}

			err = v.vs.InsertOrUpdateVin(c.Context(), dbVin)

			if err != nil {
				return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
					"error": fmt.Sprintf("Failed to submit deleteVehicle: %v", deleteVehicle),
				})
			}
			localLog.Debug().Str(logfields.VIN, deleteVehicle.Vin).Msg("Submitted deleteVehicle for VIN")
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}

func (v *VehicleController) canSubmitDeleteJob(record *dbmodels.Vin) bool {
	if record == nil {
		return false
	}

	disconnected := onboarding.IsDisconnected(record.OnboardingStatus)
	failed := onboarding.IsFailure(record.OnboardingStatus)
	pending := onboarding.IsBurnPending(record.OnboardingStatus)

	return (disconnected || failed) && !pending
}

func (v *VehicleController) GetDeleteStatusForVins(c *fiber.Ctx) error {
	params := new(VinsGetParams)
	if err := c.QueryParser(params); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Failed to parse VINs",
		})
	}

	localLog := v.logger.With().Str(logfields.FunctionName, "GetDeleteStatusForVins").Interface("validVins", params.Vins).Logger()
	localLog.Debug().Interface("vins", params.Vins).Msg("Checking Delete Status for Vins")

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
					Status:  onboarding.GetBurnStatus(dbVin.OnboardingStatus),
					Details: onboarding.GetDetailedStatus(dbVin.OnboardingStatus),
				})
			}
		}
	}

	return c.JSON(StatusForVinsResponse{
		Statuses: statuses,
	})
}
