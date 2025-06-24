package onboarding

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DIMO-Network/go-transactions"
	"github.com/DIMO-Network/go-zerodev"
	"github.com/DIMO-Network/shared/pkg/db"
	"github.com/DIMO-Network/shared/pkg/logfields"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/friendsofgo/errors"
	"github.com/riverqueue/river"
	"github.com/rs/zerolog"
	"github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"sync"
	"time"
)

type DeleteArgs struct {
	VIN           string                 `json:"vin"`
	UserOperation *zerodev.UserOperation `json:"userOperation"`
}

func (a DeleteArgs) Kind() string {
	return "delete"
}
func (a DeleteArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: false,
		},
	}
}

type DeleteWorker struct {
	settings *config.Settings
	logger   zerolog.Logger
	identity service.IdentityAPI
	dbs      *db.Store
	tr       *transactions.Client
	ws       service.SDWalletsAPI
	m        sync.RWMutex
	vendor   VendorOnboardingAPI

	river.WorkerDefaults[DeleteArgs]
}

func NewDeleteWorker(settings *config.Settings, logger zerolog.Logger, identity service.IdentityAPI, dbs *db.Store, tr *transactions.Client, ws service.SDWalletsAPI, vendor VendorOnboardingAPI) *DeleteWorker {
	return &DeleteWorker{
		settings: settings,
		logger:   logger,
		identity: identity,
		dbs:      dbs,
		tr:       tr,
		ws:       ws,
		vendor:   vendor,
	}
}

func (w *DeleteWorker) Timeout(*river.Job[DeleteArgs]) time.Duration { return 30 * time.Minute }

func (w *DeleteWorker) Work(ctx context.Context, job *river.Job[DeleteArgs]) error {
	w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Msg("Delete VIN")

	// Check if the VIN record exists
	record, err := w.GetVinRecord(ctx, job.Args.VIN)
	if err != nil {
		return err
	}

	if record.OnboardingStatus < OnboardingStatusBurnSDSuccess {
		return fmt.Errorf("insufficient disconnect status")
	}

	// Check already burned Vehicle, just return
	if record.OnboardingStatus == OnboardingStatusBurnVehicleSuccess {
		return nil
	}

	// If SD Token ID is valid - fail, can't burn
	if record.SyntheticTokenID.Valid {
		w.logger.Error().Str(logfields.VIN, job.Args.VIN).Msg("SD defined, can't burn")
		return errors.New("sd not empty")
	}

	// If Vehicle Token ID is valid - burn
	if record.VehicleTokenID.Valid {
		w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Msg("Burning Vehicle")
		_, err = w.BurnVehicleAndUpdate(ctx, record, job.Args)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *DeleteWorker) GetVinRecord(ctx context.Context, vinToSearch string) (*dbmodels.Vin, error) {
	vin, err := dbmodels.Vins(dbmodels.VinWhere.Vin.EQ(vinToSearch)).One(ctx, w.dbs.DBS().Reader)

	if err != nil {
		return nil, err
	}

	return vin, nil
}

func (w *DeleteWorker) BurnVehicleAndUpdate(ctx context.Context, record *dbmodels.Vin, args DeleteArgs) (*dbmodels.Vin, error) {
	// make sure we save status update (and possible new DD)
	defer (func() {
		_ = w.update(ctx, record, args, boil.Whitelist(dbmodels.VinColumns.OnboardingStatus, dbmodels.VinColumns.VehicleTokenID))
	})()

	w.logger.Debug().Str(logfields.VIN, args.VIN).Msg("Burning Vehicle")

	w.m.Lock()
	opResult, err := w.tr.SendSignedUserOperation(args.UserOperation, true)
	if err != nil {
		w.m.Unlock()
		w.logger.Error().Err(err).Msg("Failed to Burn Vehicle")
		record.OnboardingStatus = OnboardingStatusBurnVehicleFailure
		return nil, err
	}

	result, err := w.tr.GetBurnVehicleByOwnerResult(opResult)
	if err != nil {
		w.m.Unlock()
		w.logger.Error().Err(err).Msg("Failed to get burn Vehicle result")
		record.OnboardingStatus = OnboardingStatusBurnVehicleFailure
	}

	w.m.Unlock()

	record.VehicleTokenID = null.NewInt64(0, false)
	record.OnboardingStatus = OnboardingStatusBurnVehicleSuccess

	w.logger.Debug().Str(logfields.VIN, args.VIN).Int64(logfields.VehicleTokenID, result.VehicleNode.Int64()).Msg("Vehicle burned")

	return record, nil
}

func (w *DeleteWorker) update(ctx context.Context, record *dbmodels.Vin, args DeleteArgs, columns boil.Columns) error {
	tx, err := w.dbs.DBS().Writer.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to begin transaction")
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil && err == nil {
				w.logger.Error().Err(rbErr).Msg("Failed to rollback transaction")
			}
		}
	}()

	_, err = record.Update(ctx, w.dbs.DBS().Writer, columns)
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to update VIN record")
	}

	if err = tx.Commit(); err != nil {
		w.logger.Error().Err(err).Msg("Failed to commit transaction")
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	w.logger.Debug().Str(logfields.VIN, args.VIN).Msg("VIN record updated")

	return nil
}
