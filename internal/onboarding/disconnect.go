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
	"github.com/riverqueue/river"
	"github.com/rs/zerolog"
	"github.com/volatiletech/null/v8"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"sync"
	"time"
)

type DisconnectArgs struct {
	VIN           string                 `json:"vin"`
	UserOperation *zerodev.UserOperation `json:"userOperation"`
}

func (a DisconnectArgs) Kind() string {
	return "disconnect"
}
func (a DisconnectArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		MaxAttempts: 1,
		UniqueOpts: river.UniqueOpts{
			ByArgs: false,
		},
	}
}

type DisconnectWorker struct {
	settings *config.Settings
	logger   zerolog.Logger
	identity service.IdentityAPI
	dbs      *db.Store
	tr       *transactions.Client
	ws       service.SDWalletsAPI
	m        sync.RWMutex
	vendor   VendorOnboardingAPI

	river.WorkerDefaults[DisconnectArgs]
}

func NewDisconnectWorker(settings *config.Settings, logger zerolog.Logger, identity service.IdentityAPI, dbs *db.Store, tr *transactions.Client, ws service.SDWalletsAPI, vendor VendorOnboardingAPI) *DisconnectWorker {
	return &DisconnectWorker{
		settings: settings,
		logger:   logger,
		identity: identity,
		dbs:      dbs,
		tr:       tr,
		ws:       ws,
		vendor:   vendor,
	}
}

func (w *DisconnectWorker) Timeout(*river.Job[DisconnectArgs]) time.Duration { return 30 * time.Minute }

func (w *DisconnectWorker) Work(ctx context.Context, job *river.Job[DisconnectArgs]) error {
	w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Msg("Disconnection VIN")

	// Check if the VIN record exists
	record, err := w.GetVinRecord(ctx, job.Args.VIN)
	if err != nil {
		return err
	}

	if record.OnboardingStatus < OnboardingStatusMintSuccess {
		return fmt.Errorf("insufficient verification status")
	}

	// Check already burned SD, just return
	if record.OnboardingStatus == OnboardingStatusBurnSDSuccess {
		return nil
	}

	// Disconnect to external vendor
	record, err = w.DisconnectFromVendorAndUpdate(ctx, record, job.Args)
	if err != nil {
		return err
	}

	// If SD is valid - burn
	if record.SyntheticTokenID.Valid {
		w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Msg("Burning SD")
		_, err = w.BurnSDAndUpdate(ctx, record, job.Args)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *DisconnectWorker) GetVinRecord(ctx context.Context, vinToSearch string) (*dbmodels.Vin, error) {
	vin, err := dbmodels.Vins(dbmodels.VinWhere.Vin.EQ(vinToSearch)).One(ctx, w.dbs.DBS().Reader)

	if err != nil {
		return nil, err
	}

	return vin, nil
}

func (w *DisconnectWorker) DisconnectFromVendorAndUpdate(ctx context.Context, record *dbmodels.Vin, args DisconnectArgs) (*dbmodels.Vin, error) {
	w.logger.Debug().Str(logfields.VIN, args.VIN).Msg("Disconnecting from vendor")

	// make sure we save status update (and possible new DD)
	defer (func() { _ = w.update(ctx, record, args, boil.Whitelist(dbmodels.VinColumns.OnboardingStatus)) })()

	record.OnboardingStatus = OnboardingStatusDisconnectUnknown

	if w.settings.EnableVendorConnection {
		connection, err := w.vendor.Disconnect([]string{args.VIN})
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to disconnect from vendor")
			record.OnboardingStatus = OnboardingStatusDisconnectFailure
			return nil, err
		}

		record.DisconnectionStatus = null.String{String: "succeeded", Valid: true}
		record.ConnectionStatus = null.String{String: "", Valid: false}
		err = w.update(ctx, record, args, boil.Whitelist(dbmodels.VinColumns.ConnectionStatus, dbmodels.VinColumns.DisconnectionStatus))
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to update disconnection status")
			record.OnboardingStatus = OnboardingStatusConnectFailure
			return nil, err
		}

		w.logger.Debug().Str(logfields.VIN, args.VIN).Interface("disconnection-result", connection).Msg("Vendor disconnected")
	} else {
		w.logger.Debug().Str(logfields.VIN, args.VIN).Msg("Vendor connection is disabled, skipping")
		record.DisconnectionStatus = null.String{String: "succeeded", Valid: true}
		record.ConnectionStatus = null.String{String: "", Valid: false}
		err := w.update(ctx, record, args, boil.Whitelist(dbmodels.VinColumns.ConnectionStatus, dbmodels.VinColumns.DisconnectionStatus))
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to update disconnection status")
			record.OnboardingStatus = OnboardingStatusConnectFailure
			return nil, err
		}
	}

	record.OnboardingStatus = OnboardingStatusDisconnectSuccess

	return record, nil
}

func (w *DisconnectWorker) BurnSDAndUpdate(ctx context.Context, record *dbmodels.Vin, args DisconnectArgs) (*dbmodels.Vin, error) {
	// make sure we save status update (and possible new DD)
	defer (func() {
		_ = w.update(ctx, record, args, boil.Whitelist(dbmodels.VinColumns.OnboardingStatus, dbmodels.VinColumns.SyntheticTokenID, dbmodels.VinColumns.WalletIndex))
	})()

	w.logger.Debug().Str(logfields.VIN, args.VIN).Msg("Burning SD")

	w.m.Lock()
	opResult, err := w.tr.SendSignedUserOperation(args.UserOperation, true)
	if err != nil {
		w.m.Unlock()
		w.logger.Error().Err(err).Msg("Failed to Burn SD")
		record.OnboardingStatus = OnboardingStatusBurnSDFailure
		return nil, err
	}

	result, err := w.tr.GetBurnSDByOwnerResult(opResult)
	if err != nil {
		w.m.Unlock()
		w.logger.Error().Err(err).Msg("Failed to get burn SD result")
		record.OnboardingStatus = OnboardingStatusBurnSDFailure
	}

	w.m.Unlock()

	record.WalletIndex = null.NewInt64(0, false)
	record.SyntheticTokenID = null.NewInt64(0, false)
	record.OnboardingStatus = OnboardingStatusBurnSDSuccess

	w.logger.Debug().Str(logfields.VIN, args.VIN).Int64("syntheticDeviceTokenId", result.SyntheticDeviceNode.Int64()).Msg("SD burned")

	return record, nil
}

func (w *DisconnectWorker) update(ctx context.Context, record *dbmodels.Vin, args DisconnectArgs, columns boil.Columns) error {
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
