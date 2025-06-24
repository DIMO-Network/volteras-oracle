package service

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DIMO-Network/shared/pkg/db"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/friendsofgo/errors"
	"github.com/rs/zerolog"
	"github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"github.com/volatiletech/sqlboiler/v4/queries/qm"
)

type Vehicle struct {
	pdb    *db.Store
	logger *zerolog.Logger
}

var ErrVehicleNotFound = errors.New("vehicle not found")

// NewVehicleService creates a new instance of Vehicle.
func NewVehicleService(pdb *db.Store, logger *zerolog.Logger) *Vehicle {
	return &Vehicle{
		pdb:    pdb,
		logger: logger,
	}
}

// GetVehicleByVin retrieves a vehicle by its VIN.
func (ds *Vehicle) GetVehicleByVin(ctx context.Context, vehicleID string) (*dbmodels.Vin, error) {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to begin transaction for vehicle %s", vehicleID)
		return nil, err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("GetVehicleByVin: Failed to rollback transaction for vehicle %s", vehicleID)
			}
		} else {
			if cmErr := tx.Commit(); cmErr != nil {
				ds.logger.Error().Err(cmErr).Msgf("GetVehicleByVin: Failed to commit transaction for vehicle %s", vehicleID)
			}
		}
	}()

	vin, err := dbmodels.Vins(dbmodels.VinWhere.Vin.EQ(vehicleID)).One(ctx, tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVehicleNotFound
		}
		ds.logger.Error().Err(err).Msgf("Failed to check if vehicle %s has been processed", vehicleID)
		return nil, err
	}

	return vin, nil
}

// GetVehiclesByVins retrieves vehicles by their VINs.
func (ds *Vehicle) GetVehiclesByVins(ctx context.Context, vehicleIDs []string) (dbmodels.VinSlice, error) {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msg("GetVehiclesByVins: Failed to begin transaction for vehicles")
		return nil, err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("GetVehiclesByVins: Failed to rollback transaction for vehicles")
			}
		} else {
			if cmErr := tx.Commit(); cmErr != nil {
				ds.logger.Error().Err(cmErr).Msgf("GetVehiclesByVins: Failed to commit transaction for vehicles")
			}
		}
	}()

	vins, err := dbmodels.Vins(dbmodels.VinWhere.Vin.IN(vehicleIDs)).All(ctx, tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVehicleNotFound
		}
		ds.logger.Error().Err(err).Msgf("GetVehiclesByVins: Failed to check if vehicles have been processed")
		return nil, err
	}

	return vins, nil
}

// GetMintableVehiclesByVins retrieves vehicles available for minting SD (or vehicle + SD) by their VINs.
func (ds *Vehicle) GetVehiclesByVinsAndOnboardingStatus(ctx context.Context, vehicleIDs []string, status int) (dbmodels.VinSlice, error) {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msg("GetVehiclesByVinsAndOnboardingStatus: Failed to begin transaction for vehicles")
		return nil, err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("GetVehiclesByVinsAndOnboardingStatus: Failed to rollback transaction for vehicles")
			}
		} else {
			if cmErr := tx.Commit(); cmErr != nil {
				ds.logger.Error().Err(cmErr).Msgf("GetVehiclesByVinsAndOnboardingStatus: Failed to commit transaction for vehicles")
			}
		}
	}()

	vins, err := dbmodels.Vins(
		dbmodels.VinWhere.Vin.IN(vehicleIDs),
		dbmodels.VinWhere.OnboardingStatus.EQ(status),
	).All(ctx, tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVehicleNotFound
		}
		ds.logger.Error().Err(err).Msgf("GetVehiclesByVinsAndOnboardingStatus: Failed to fetch vehicles")
		return nil, err
	}

	return vins, nil
}

func (ds *Vehicle) GetVehiclesByVinsAndOnboardingStatusRange(ctx context.Context, vehicleIDs []string, minStatus, maxStatus int, additionalStatuses []int) (dbmodels.VinSlice, error) {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msg("GetVehiclesByVinsAndOnboardingStatusRange: Failed to begin transaction for vehicles")
		return nil, err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("GetVehiclesByVinsAndOnboardingStatusRange: Failed to rollback transaction for vehicles")
			}
		} else {
			if cmErr := tx.Commit(); cmErr != nil {
				ds.logger.Error().Err(cmErr).Msgf("GetVehiclesByVinsAndOnboardingStatusRange: Failed to commit transaction for vehicles")
			}
		}
	}()

	var vins dbmodels.VinSlice

	if len(additionalStatuses) > 0 {
		vinInRange := dbmodels.VinWhere.Vin.IN(vehicleIDs)
		statusInRange := qm.Expr(dbmodels.VinWhere.OnboardingStatus.GTE(minStatus), dbmodels.VinWhere.OnboardingStatus.LTE(maxStatus))
		statusInRangeOrAdditional := qm.Expr(statusInRange, qm.Or2(dbmodels.VinWhere.OnboardingStatus.IN(additionalStatuses)))

		vins, err = dbmodels.Vins(
			vinInRange,
			statusInRangeOrAdditional,
		).All(ctx, tx)
	} else {
		vins, err = dbmodels.Vins(
			dbmodels.VinWhere.Vin.IN(vehicleIDs),
			dbmodels.VinWhere.OnboardingStatus.GTE(minStatus),
			dbmodels.VinWhere.OnboardingStatus.LTE(maxStatus),
		).All(ctx, tx)
	}
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrVehicleNotFound
		}
		ds.logger.Error().Err(err).Msgf("GetVehiclesByVinsAndOnboardingStatusRange: Failed to fetch vehicles")
		return nil, err
	}

	return vins, nil
}

// GetVehicleByExternalID retrieves a vehicle by its external ID.
func (ds *Vehicle) GetVehicleByExternalID(ctx context.Context, externalID string) (*dbmodels.Vin, error) {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to begin transaction for external ID %s", externalID)
		return nil, err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("Failed to rollback transaction for external ID %s", externalID)
			}
		}
	}()

	externalIDNull := null.StringFrom(externalID)
	vin, err := dbmodels.Vins(dbmodels.VinWhere.ExternalID.EQ(externalIDNull)).One(ctx, tx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("vehicle not found")
		}
		ds.logger.Error().Err(err).Msgf("Failed to check if vehicle with external ID %s has been processed", externalID)
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to commit transaction for external ID %s", externalID)
		return nil, err
	}

	return vin, nil
}

// InsertVinToDB inserts a new VIN record into the database.
func (ds *Vehicle) InsertVinToDB(ctx context.Context, vin *dbmodels.Vin) error {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to begin transaction for vehicle %s", vin.Vin)
		return err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("InsertVinToDB: Failed to rollback transaction for vehicle %s", vin.Vin)
			}
		}
	}()

	err = vin.Insert(ctx, tx, boil.Infer())
	if err != nil {
		return fmt.Errorf("failed to insert VIN record: %v", err)
	}

	if err := tx.Commit(); err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to commit transaction for vehicle %s", vin.Vin)
		return err
	}

	return nil
}

// InsertOrUpdateVin inserts a new VIN record into the database.
func (ds *Vehicle) InsertOrUpdateVin(ctx context.Context, vin *dbmodels.Vin) error {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to begin transaction for vehicle %s", vin.Vin)
		return err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("InsertVinToDB: Failed to rollback transaction for vehicle %s", vin.Vin)
			}
		}
	}()

	err = vin.Upsert(ctx, tx, true, []string{"vin"}, boil.Infer(), boil.Infer())
	if err != nil {
		return fmt.Errorf("failed to insert VIN record: %v", err)
	}

	if err := tx.Commit(); err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to commit transaction for vehicle %s", vin.Vin)
		return err
	}

	return nil
}

// UpdateEnrollmentStatus updates the enrollment status and external ID of a VIN record.
func (ds *Vehicle) UpdateEnrollmentStatus(ctx context.Context, vin, status, externalID string, error *models.OperationError) error {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("UpdateEnrollmentStatus: Failed to rollback transaction for vehicle %s", vin)
			}
		}
	}()

	vinRecord, err := dbmodels.Vins(dbmodels.VinWhere.Vin.EQ(vin)).One(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to fetch VIN record: %w", err)
	}

	vinRecord.ConnectionStatus = null.StringFrom(status)
	vinRecord.ExternalID = null.StringFrom(externalID)
	if status == "succeeded" {
		vinRecord.DisconnectionStatus = null.String{String: "", Valid: false}
	}

	if error != nil {
		vinRecord.OperationErrorType = null.StringFrom(error.Type)
		vinRecord.OperationErrorCode = null.StringFrom(error.Code)
		vinRecord.OperationErrorDescription = null.StringFrom(error.Description)
	} else {
		vinRecord.OperationErrorType = null.String{String: "", Valid: false}
		vinRecord.OperationErrorCode = null.String{String: "", Valid: false}
		vinRecord.OperationErrorDescription = null.String{String: "", Valid: false}
	}

	if _, err := vinRecord.Update(ctx, tx, boil.Whitelist(
		dbmodels.VinColumns.ConnectionStatus,
		dbmodels.VinColumns.DisconnectionStatus,
		dbmodels.VinColumns.ExternalID,
		dbmodels.VinColumns.OperationErrorType,
		dbmodels.VinColumns.OperationErrorCode,
		dbmodels.VinColumns.OperationErrorDescription,
	)); err != nil {
		return fmt.Errorf("failed to update VIN record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to commit transaction for vehicle %s", vin)
		return err
	}

	return nil
}

// UpdateUnenrollmentStatus updates the unenrollment status of a VIN record.
func (ds *Vehicle) UpdateUnenrollmentStatus(ctx context.Context, vin, status string, error *models.OperationError) error {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				ds.logger.Error().Err(rbErr).Msgf("UpdateUnenrollmentStatus: Failed to rollback transaction for vehicle %s", vin)
			}
		}
	}()

	vinRecord, err := dbmodels.Vins(dbmodels.VinWhere.Vin.EQ(vin)).One(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to fetch VIN record: %w", err)
	}

	vinRecord.DisconnectionStatus = null.StringFrom(status)
	if status == "succeeded" {
		vinRecord.ConnectionStatus = null.String{String: "", Valid: false}
		vinRecord.ExternalID = null.String{String: "", Valid: false}
	}

	if error != nil {
		vinRecord.OperationErrorType = null.StringFrom(error.Type)
		vinRecord.OperationErrorCode = null.StringFrom(error.Code)
		vinRecord.OperationErrorDescription = null.StringFrom(error.Description)
	} else {
		vinRecord.OperationErrorType = null.String{String: "", Valid: false}
		vinRecord.OperationErrorCode = null.String{String: "", Valid: false}
		vinRecord.OperationErrorDescription = null.String{String: "", Valid: false}
	}

	if _, err := vinRecord.Update(ctx, tx, boil.Whitelist(
		dbmodels.VinColumns.ConnectionStatus,
		dbmodels.VinColumns.DisconnectionStatus,
		dbmodels.VinColumns.ExternalID,
		dbmodels.VinColumns.OperationErrorType,
		dbmodels.VinColumns.OperationErrorCode,
		dbmodels.VinColumns.OperationErrorDescription,
	)); err != nil {
		return fmt.Errorf("failed to update VIN record: %w", err)
	}

	if err := tx.Commit(); err != nil {
		ds.logger.Error().Err(err).Msgf("Failed to commit transaction for vehicle %s", vin)
		return err
	}

	return nil
}

// GetVinsByTokenIDs retrieves VINs where VehicleTokenID is in the provided token IDs.
func (ds *Vehicle) GetVinsByTokenIDs(ctx context.Context, tokenIDsToCheck []int64) (dbmodels.VinSlice, error) {
	vins, err := dbmodels.Vins(dbmodels.VinWhere.VehicleTokenID.IN(tokenIDsToCheck)).All(ctx, ds.pdb.DBS().Reader)
	if err != nil {
		ds.logger.Error().Err(err).Msg("Failed to get VINs by token IDs")
		return nil, fmt.Errorf("failed to get VINs by token IDs: %w", err)
	}
	return vins, nil
}

// GetVehiclesFromDB retrieves all VINs from the database.
func (ds *Vehicle) GetVehiclesFromDB(ctx context.Context) (dbmodels.VinSlice, error) {
	tx, err := ds.pdb.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		ds.logger.Error().Err(err).Msg("Failed to begin transaction")
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && err == nil {
				ds.logger.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()

	vins, err := dbmodels.Vins().All(ctx, tx)
	if err != nil {
		ds.logger.Error().Err(err).Msg("Failed to get VINs")
		return nil, fmt.Errorf("failed to get VINs: %w", err)
	}

	if err := tx.Commit(); err != nil {
		ds.logger.Error().Err(err).Msg("Failed to commit transaction")
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return vins, nil
}
