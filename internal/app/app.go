package app

import (
	"errors"
	"github.com/DIMO-Network/go-transactions"
	"github.com/DIMO-Network/shared/pkg/middleware/metrics"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/DIMO-Network/volteras-oracle/internal/controllers"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	jwtware "github.com/gofiber/contrib/jwt"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	fiberrecover "github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"
	"github.com/rs/zerolog"
	"strconv"
)

func App(settings *config.Settings, logger *zerolog.Logger, db *service.Vehicle, riverClient *river.Client[pgx.Tx], ws service.SDWalletsAPI, tr *transactions.Client) *fiber.App {
	if tr == nil {
		logger.Fatal().Err(errors.New("tr transactions.Client is nil"))
	}
	// all the fiber logic here, routes, authorization
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			return ErrorHandler(c, err, logger)
		},
		DisableStartupMessage:    true,
		ReadBufferSize:           16000,
		BodyLimit:                5 * 1024 * 1024,
		EnableSplittingOnParsers: true,
	})
	app.Use(metrics.HTTPMetricsMiddleware)

	app.Use(fiberrecover.New(fiberrecover.Config{
		Next:              nil,
		EnableStackTrace:  true,
		StackTraceHandler: nil,
	}))

	app.Use(cors.New(cors.Config{
		AllowOrigins:     "https://localdev.dimo.org:3008", // localhost development
		AllowMethods:     "GET,POST,PUT,DELETE,OPTIONS",
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization",
		AllowCredentials: true,
	}))

	app.Get("/health", healthCheck)

	identityService := service.NewIdentityAPIService(*logger, *settings)
	vehiclesCtrl := controllers.NewVehiclesController(settings, logger, identityService, db, riverClient, ws, tr)

	// assumes frontend has used Login With DIMO and has a JWT from DIMO.
	jwtAuth := jwtware.New(jwtware.Config{
		JWKSetURLs: []string{settings.JwtKeySetURL},
	})
	// get all vehicles in the database for frontend
	app.Get("/v1/vehicles", jwtAuth, vehiclesCtrl.GetVehicles)

	// gets verification (VIN decoding and vendor support check) statuses
	app.Get("/v1/vehicle/verify", jwtAuth, vehiclesCtrl.GetVerificationStatusForVins)
	// handles decoding the VIN to be onboarded and checking if the vendor supports this VIN. Optional.
	app.Post("/v1/vehicle/verify", jwtAuth, vehiclesCtrl.SubmitVerificationForVins)

	// gets minting status
	app.Get("/v1/vehicle/mint/status", jwtAuth, vehiclesCtrl.GetMintStatusForVins)
	// gets the payload to be signed for minting by the frontend (using passkey)
	app.Get("/v1/vehicle/mint", jwtAuth, vehiclesCtrl.GetMintDataForVins)
	// submits the passkey signed minting payload to the backend
	app.Post("/v1/vehicle/mint", jwtAuth, vehiclesCtrl.SubmitMintDataForVins)

	// gets disconnection status
	app.Get("/v1/vehicle/disconnect/status", jwtAuth, vehiclesCtrl.GetDisconnectStatusForVins)
	// gets the payload to be signed for disconnecting by the frontend (using passkey)
	app.Get("/v1/vehicle/disconnect", jwtAuth, vehiclesCtrl.GetDisconnectDataForVins)
	// submits the passkey signed disconnecting payload to the backend
	app.Post("/v1/vehicle/disconnect", jwtAuth, vehiclesCtrl.SubmitDisconnectDataForVins)

	// gets vehicle deletion status
	app.Get("/v1/vehicle/delete/status", jwtAuth, vehiclesCtrl.GetDeleteStatusForVins)
	// gets the payload to be signed for deleting a vehicle by the frontend (using passkey)
	app.Get("/v1/vehicle/delete", jwtAuth, vehiclesCtrl.GetDeleteDataForVins)
	// submits the passkey signed delete vehicle payload to the backend
	app.Post("/v1/vehicle/delete", jwtAuth, vehiclesCtrl.SubmitDeleteDataForVins)

	// get a specific vehicle by ID (could be VIN or whatever identifier)
	app.Get("/v1/vehicle/:externalID", jwtAuth, vehiclesCtrl.GetVehicleByExternalID)
	// submits vehicles to be registered by the backend
	app.Post("/v1/vehicle/register", jwtAuth, vehiclesCtrl.RegisterVehicle)

	return app
}

func healthCheck(c *fiber.Ctx) error {
	res := map[string]interface{}{
		"data": "Server is up and running",
	}

	err := c.JSON(res)

	if err != nil {
		return err
	}

	return nil
}

// ErrorHandler custom handler to log recovered errors using our logger and return json instead of string
func ErrorHandler(c *fiber.Ctx, err error, logger *zerolog.Logger) error {
	code := fiber.StatusInternalServerError // Default 500 statuscode

	var e *fiber.Error
	isFiberErr := errors.As(err, &e)
	if isFiberErr {
		// Override status code if fiber.Error type
		code = e.Code
	}
	c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
	codeStr := strconv.Itoa(code)

	if code != fiber.StatusNotFound {
		logger.Err(err).Str("httpStatusCode", codeStr).
			Str("httpMethod", c.Method()).
			Str("httpPath", c.Path()).
			Msg("caught an error from http request")
	}
	// return an opaque error if we're in a higher level environment and we haven't specified an fiber type err.
	//if !isFiberErr && isProduction {
	//	err = fiber.NewError(fiber.StatusInternalServerError, "Internal error")
	//}

	return c.Status(code).JSON(ErrorRes{
		Code:    code,
		Message: err.Error(),
	})
}

type ErrorRes struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}
