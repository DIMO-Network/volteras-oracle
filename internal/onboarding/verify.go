package onboarding

import (
	"context"
	"database/sql"
	"errors"
	"github.com/DIMO-Network/shared/pkg/db"
	"github.com/DIMO-Network/shared/pkg/logfields"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/riverqueue/river"
	"github.com/rs/zerolog"
	"github.com/volatiletech/sqlboiler/v4/boil"
	"time"
)

type VerifyArgs struct {
	VIN         string `json:"vin"`
	CountryCode string `json:"countryCode"`
}

func (a VerifyArgs) Kind() string {
	return "verify"
}

func (VerifyArgs) InsertOpts() river.InsertOpts {
	return river.InsertOpts{
		UniqueOpts: river.UniqueOpts{
			ByArgs: false,
		},
	}
}

type VerifyWorker struct {
	settings *config.Settings
	logger   zerolog.Logger
	identity service.IdentityAPI
	os       *service.OracleService
	dd       service.DeviceDefinitionsAPI
	dbs      *db.Store
	vendor   VendorOnboardingAPI

	river.WorkerDefaults[VerifyArgs]
}

func NewVerifyWorker(settings *config.Settings, logger zerolog.Logger, identity service.IdentityAPI, dd service.DeviceDefinitionsAPI, os *service.OracleService, dbs *db.Store, vendor VendorOnboardingAPI) *VerifyWorker {
	return &VerifyWorker{
		settings: settings,
		logger:   logger,
		identity: identity,
		os:       os,
		dd:       dd,
		dbs:      dbs,
		vendor:   vendor,
	}
}

func (w *VerifyWorker) Work(ctx context.Context, job *river.Job[VerifyArgs]) error {
	w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Str(logfields.CountryCode, job.Args.CountryCode).Msg("Verifying VIN")

	// Check if the VIN already exists, create the record if not
	record, err := w.GetOrCreateVinRecord(ctx, job.Args)
	if err != nil {
		return err
	}

	w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Str(logfields.CountryCode, job.Args.CountryCode).Msgf("Onboarding status: %d", record.OnboardingStatus)
	if record.OnboardingStatus == OnboardingStatusVendorValidationSuccess {
		w.logger.Debug().Str(logfields.VIN, job.Args.VIN).Str(logfields.CountryCode, job.Args.CountryCode).Msg("Verification already done, skipping")
		return nil
	}

	err = w.DecodeVinAndUpdate(record, job.Args)
	if err != nil {
		return err
	}

	// Validate with external Vendor
	err = w.ValidateWithExternalVendorAndUpdate(record, job.Args)
	if err != nil {
		return err
	}

	return nil
}

func (w *VerifyWorker) GetOrCreateVinRecord(ctx context.Context, args VerifyArgs) (*dbmodels.Vin, error) {
	vin, err := dbmodels.Vins(dbmodels.VinWhere.Vin.EQ(args.VIN)).One(ctx, w.dbs.DBS().Reader)

	if err != nil {
		// fail only in case of an actual error, no rows are still ok
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}

	if vin != nil {
		return vin, nil
	}

	vin = &dbmodels.Vin{
		Vin:              args.VIN,
		OnboardingStatus: OnboardingStatusDecodingUnknown,
	}

	err = vin.Insert(ctx, w.dbs.DBS().Writer, boil.Infer())
	if err != nil {
		return nil, err
	}

	return vin, nil
}

func (w *VerifyWorker) DecodeVinAndUpdate(record *dbmodels.Vin, args VerifyArgs) error {
	// make sure we save status update (and possible new DD)
	defer (func() { _ = w.update(record, args) })()

	if record.OnboardingStatus < OnboardingStatusDecodingSuccess || record.DeviceDefinitionID.IsZero() || len(record.DeviceDefinitionID.String) == 0 {
		w.logger.Debug().Str(logfields.VIN, args.VIN).Str(logfields.CountryCode, args.CountryCode).Msg("Decoding VIN")
		record.OnboardingStatus = OnboardingStatusDecodingPending
		_ = w.update(record, args)

		decoded, err := w.dd.DecodeVin(args.VIN, args.CountryCode)
		if err != nil {
			record.OnboardingStatus = OnboardingStatusDecodingFailure
			return err
		}
		w.logger.Debug().Str(logfields.VIN, args.VIN).Str(logfields.CountryCode, args.CountryCode).Str(logfields.DefinitionID, decoded.DeviceDefinitionID).Msg("VIN decoded")

		dd, err := w.getOrWaitForDeviceDefinition(decoded.DeviceDefinitionID)
		if err != nil {
			record.OnboardingStatus = OnboardingStatusDecodingFailure
			return err
		}
		w.logger.Debug().Str(logfields.VIN, args.VIN).Str(logfields.CountryCode, args.CountryCode).Str(logfields.DefinitionID, dd.DeviceDefinitionID).Interface("dd", dd).Msg("DD fetched from identity")

		record.DeviceDefinitionID.SetValid(dd.DeviceDefinitionID)
	} else {
		w.logger.Debug().Str(logfields.VIN, args.VIN).Str(logfields.CountryCode, args.CountryCode).Str(logfields.DefinitionID, record.DeviceDefinitionID.String).Msg("VIN already decoded")
	}

	record.OnboardingStatus = OnboardingStatusDecodingSuccess

	return nil
}

func (w *VerifyWorker) ValidateWithExternalVendorAndUpdate(record *dbmodels.Vin, args VerifyArgs) error {
	// make sure we save status update (and possible new DD)
	defer (func() { _ = w.update(record, args) })()

	w.logger.Debug().Str(logfields.VIN, record.Vin).Msg("Validating with external vendor")

	record.OnboardingStatus = OnboardingStatusVendorValidationUnknown

	if w.settings.EnableVendorCapabilityCheck {
		validation, err := w.vendor.Validate([]string{args.VIN})
		if err != nil {
			w.logger.Error().Err(err).Msg("Failed to validate VIN")
			record.OnboardingStatus = OnboardingStatusVendorValidationFailure
			return err
		}

		w.logger.Debug().Str(logfields.VIN, record.Vin).Interface("validation", validation).Msg("VIN validated")

		if validation[0].Status == "notCapable " || validation[0].Status == "noDataFound" {
			w.logger.Error().Str("vin", args.VIN).Str("status", validation[0].Status).Msg("VIN validation failed")
			record.OnboardingStatus = OnboardingStatusVendorValidationFailure
			return errors.New("vin validation failed")
		}

		w.logger.Debug().Str(logfields.VIN, record.Vin).Interface("verification", validation[0]).Msg("VIN validation successful")
		w.logger.Debug().Str(logfields.VIN, record.Vin).Msg("VIN validation successful")
	} else {
		w.logger.Debug().Str(logfields.VIN, record.Vin).Msg("Vendor capability check disabled, skipping validation")
	}

	record.OnboardingStatus = OnboardingStatusVendorValidationSuccess

	return nil
}

func (w *VerifyWorker) getOrWaitForDeviceDefinition(deviceDefinitionID string) (*models.DeviceDefinition, error) {
	w.logger.Debug().Str(logfields.DefinitionID, deviceDefinitionID).Msg("Waiting for device definition")
	for i := 0; i < 12; i++ {
		definition, err := w.identity.FetchDeviceDefinitionByID(deviceDefinitionID)
		if err != nil || definition == nil || definition.DeviceDefinitionID == "" {
			time.Sleep(5 * time.Second)
			w.logger.Debug().Str(logfields.DefinitionID, deviceDefinitionID).Msgf("Still waiting, retry %d", i+1)
			continue
		}
		return definition, nil
	}

	return nil, errors.New("device definition not found")
}

func (w *VerifyWorker) update(record *dbmodels.Vin, args VerifyArgs) error {
	_, err := record.Update(context.Background(), w.dbs.DBS().Writer, boil.Infer())
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to update VIN record")
	}

	w.logger.Debug().Str(logfields.VIN, args.VIN).Msg("VIN record updated")

	return nil
}
