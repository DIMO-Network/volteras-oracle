package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/DIMO-Network/go-transactions"
	"github.com/DIMO-Network/shared/pkg/db"
	ssetings "github.com/DIMO-Network/shared/pkg/settings"
	"github.com/DIMO-Network/volteras-oracle/internal/app"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/DIMO-Network/volteras-oracle/internal/kafka"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/onboarding"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/adaptor"
	"github.com/google/subcommands"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/rs/zerolog"
	"golang.org/x/sync/errgroup"
	"os"
	"os/signal"
	"strings"
)

func main() {
	logger := zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().
		Timestamp().
		Str("app", "volteras-oracle").
		Logger()

	settings, err := ssetings.LoadConfig[config.Settings]("settings.yaml")
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load settings")
	}

	logLevel, err := zerolog.ParseLevel(settings.LogLevel)
	if err != nil {
		logger.Fatal().Err(err).Msg("Couldn't parse log level setting.")
	}
	zerolog.SetGlobalLevel(logLevel)
	logger = logger.Level(logLevel)

	// new context that cancels on program interrupt
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	// connect to DB and make sure it is available in reasonable time
	pdb := db.NewDbConnectionFromSettings(ctx, &settings.DB, true)
	pdb.WaitForDB(logger)

	// todo CLI subcommmand for testing volteras api
	// CLI commands
	subcommands.Register(subcommands.HelpCommand(), "")
	subcommands.Register(subcommands.FlagsCommand(), "")
	subcommands.Register(subcommands.CommandsCommand(), "")
	if len(os.Args) > 1 {
		// CLI only mode
		subcommands.Register(&migrateDBCmd{logger: logger, settings: settings, pdb: pdb}, "database")

		flag.Parse()
		os.Exit(int(subcommands.Execute(ctx)))
	}

	transactionsClient, err := onboarding.NewTransactionsClient(&settings)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create transactions client")
	}

	walletService := service.NewSDWalletsService(ctx, logger, settings)
	if walletService == nil {
		logger.Fatal().Err(err).Msg("Failed to create SD Wallets service")
	}

	monApp := createMonitoringServer()
	group, gCtx := errgroup.WithContext(ctx)
	vehicleService := service.NewVehicleService(&pdb, &logger)
	identityService := service.NewIdentityAPIService(logger, settings)
	deviceDefinitionsService := service.NewDeviceDefinitionsAPIService(logger, settings)
	oracleService, err := service.NewOracleService(ctx, logger, settings, vehicleService)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create Oracle service")
	}

	enrollmentChannel := make(chan models.OperationMessage, 100)
	vendorOnboardingService := onboarding.NewExternalOnboardingService(&settings, vehicleService, &logger, enrollmentChannel)

	riverClient, _, dbPool, err := createRiverClientWithWorkersAndPool(gCtx, logger, &settings, identityService, deviceDefinitionsService, oracleService, &pdb, transactionsClient, walletService, vendorOnboardingService)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create river client, workers and db pool")
	}
	defer dbPool.Close()

	runRiver(gCtx, logger, riverClient, group)

	webAPI := app.App(&settings, &logger, vehicleService, riverClient, walletService, transactionsClient)

	// start the Web Api
	logger.Info().Str("port", settings.MonitoringPort).Msgf("Starting monitoring server %s", settings.MonitoringPort)
	runFiber(gCtx, monApp, ":"+settings.MonitoringPort, group)
	logger.Info().Str("port", settings.Port).Msgf("Starting web server %s", settings.Port)
	runFiber(gCtx, webAPI, ":"+settings.Port, group)

	kafkaBrokers := strings.Split(settings.KafkaBrokers, ",")

	if settings.IsTelemetryConsumerEnabled {
		// Setup consumer for UnbufferedTelemetryTopic
		err = kafka.SetupKafkaConsumer(
			ctx,
			&logger,
			kafkaBrokers,
			settings.UnbufferedTelemetryTopic,
			settings.UnbufferedTelemetryConsumerGroup,
			kafka.MessageHandlerUnbuffered{Logger: &logger, OracleService: oracleService},
		)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to setup consumer for UnbufferedTelemetryTopic")
		}
	}

	if settings.IsOperationsConsumerEnabled {
		// Setup consumer for OperationsTopic
		err = kafka.SetupKafkaConsumer(
			ctx,
			&logger,
			kafkaBrokers,
			settings.OperationsTopic,
			settings.OperationsConsumerGroup,
			kafka.MessageHandlerOperations{Logger: &logger, OracleService: oracleService, EnrollmentChannel: enrollmentChannel},
		)
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to setup consumer for OperationsTopic")
		}
	}

	if err = group.Wait(); err != nil {
		logger.Fatal().Err(err).Msg("Server failed.")
	}
	logger.Info().Msg("Server stopped.")
}

func runFiber(ctx context.Context, fiberApp *fiber.App, addr string, group *errgroup.Group) {
	group.Go(func() error {
		if err := fiberApp.Listen(addr); err != nil {
			return fmt.Errorf("failed to start server: %w", err)
		}
		return nil
	})
	group.Go(func() error {
		<-ctx.Done()
		if err := fiberApp.Shutdown(); err != nil {
			return fmt.Errorf("failed to shutdown server: %w", err)
		}
		return nil
	})
}

// createMonitoringServer meant for prometheus / openmetrics scraping.
func createMonitoringServer() *fiber.App {
	monApp := fiber.New(fiber.Config{DisableStartupMessage: true})

	monApp.Get("/", func(_ *fiber.Ctx) error { return nil })
	monApp.Get("/metrics", adaptor.HTTPHandler(promhttp.Handler()))

	return monApp
}

// createRiverClientWithWorkersAndPool we use the river job client to orchestrate onboarding steps for a VIN
func createRiverClientWithWorkersAndPool(ctx context.Context, logger zerolog.Logger, settings *config.Settings, identityService service.IdentityAPI, dd service.DeviceDefinitionsAPI, os *service.OracleService, dbs *db.Store, tr *transactions.Client, ws service.SDWalletsAPI, onboardingService onboarding.VendorOnboardingAPI) (*river.Client[pgx.Tx], *river.Workers, *pgxpool.Pool, error) {
	workers := river.NewWorkers()
	verifyWorker := onboarding.NewVerifyWorker(settings, logger, identityService, dd, os, dbs, onboardingService)
	onboardingWorker := onboarding.NewOnboardingWorker(settings, logger, identityService, dbs, tr, ws, onboardingService)
	disconnectWorker := onboarding.NewDisconnectWorker(settings, logger, identityService, dbs, tr, ws, onboardingService)
	deleteWorker := onboarding.NewDeleteWorker(settings, logger, identityService, dbs, tr, ws, onboardingService)

	err := river.AddWorkerSafely(workers, verifyWorker)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to add verify worker")
		return nil, nil, nil, err
	}
	logger.Debug().Msg("Added verify worker")

	err = river.AddWorkerSafely(workers, onboardingWorker)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to add onboarding worker")
		return nil, nil, nil, err
	}
	logger.Debug().Msg("Added onboarding worker")

	err = river.AddWorkerSafely(workers, disconnectWorker)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to add disconnect worker")
		return nil, nil, nil, err
	}
	logger.Debug().Msg("Added disconnect worker")

	err = river.AddWorkerSafely(workers, deleteWorker)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to add delete worker")
		return nil, nil, nil, err
	}
	logger.Debug().Msg("Added delete worker")

	dbURL := settings.DB.BuildConnectionString(true)
	dbPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to database")
		return nil, nil, nil, err
	}

	logger.Debug().Msg("DB pool for workers created")

	riverClient, err := river.NewClient(riverpgxv5.New(dbPool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: 100},
		},
		Workers: workers,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create river client")
		return nil, nil, nil, err
	}

	return riverClient, workers, dbPool, err
}

func runRiver(ctx context.Context, logger zerolog.Logger, riverClient *river.Client[pgx.Tx], group *errgroup.Group) {
	runCtx := context.Background()

	group.Go(func() error {
		logger.Debug().Msg("Starting river client")
		if err := riverClient.Start(runCtx); err != nil {
			logger.Fatal().Err(err).Msg("failed to start river client")
		}
		return nil
	})
	group.Go(func() error {
		<-ctx.Done()
		logger.Debug().Msg("Stopping river client")
		if err := riverClient.Stop(runCtx); err != nil {
			return fmt.Errorf("failed stop river client: %w", err)
		}
		return nil
	})
}
