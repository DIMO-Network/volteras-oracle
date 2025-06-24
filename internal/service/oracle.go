package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/DIMO-Network/volteras-oracle/internal/convert"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/patrickmn/go-cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/rs/zerolog"
	"net/http"
	"time"
)

type OracleService struct {
	Ctx             context.Context
	dimoNodeAPISvc  DimoNodeAPI
	identityService IdentityAPI
	logger          zerolog.Logger
	settings        config.Settings
	stop            chan bool
	Db              *Vehicle
	cache           *cache.Cache
}

// Stop is used only for functional tests
func (cs *OracleService) Stop() {
	cs.stop <- true
}

func NewOracleService(ctx context.Context, logger zerolog.Logger, settings config.Settings, db *Vehicle) (*OracleService, error) {
	// Initialize the dimo node service
	dimoNodeAPISvc := NewDimoNodeAPIService(logger, settings)

	// Initialize the identity service
	identityService := NewIdentityAPIService(logger, settings)

	// Initialize cache with a default expiration time of 10 minutes and cleanup interval of 15 minutes
	c := cache.New(10*time.Minute, 15*time.Minute)

	cs := &OracleService{
		Ctx:             ctx,
		dimoNodeAPISvc:  dimoNodeAPISvc,
		identityService: identityService,
		logger:          logger,
		settings:        settings,
		Db:              db,
		cache:           c,
	}

	return cs, nil
}

func ParseCloudEvent(msg []byte) (*cloudevent.CloudEvent[json.RawMessage], error) {
	// Unmarshal into CloudEvent struct
	var telemetry cloudevent.CloudEvent[json.RawMessage]
	err := json.Unmarshal(msg, &telemetry)
	if err != nil {
		return nil, err
	}

	// Validate the CloudEvent
	if telemetry.Data == nil || telemetry.Type == "" {
		return nil, fmt.Errorf("invalid CloudEvent: missing required fields, data: %s, type: %s", telemetry.Data, telemetry.Type)
	}

	return &telemetry, nil
}

func (cs *OracleService) HandleDeviceByVIN(msg interface{}) error {
	cs.logger.Debug().Msgf("Received message: %s", msg)

	// Ensure msg is of type []byte
	msgBytes, ok := msg.([]byte)
	if !ok {
		err := fmt.Errorf("message is not of type []byte: %T", msg)
		cs.logger.Debug().Err(err).Msg("Invalid message type.")
		return err
	}

	// Attempt to cast the message to a CloudEvent
	cloudEvent, err := ParseCloudEvent(msgBytes)

	if err != nil {
		// Log the error and return
		cs.logger.Debug().Err(err).Msg("Failed to parse message as CloudEvent.")
		return err
	}

	// Query GetDeviceByVIN function
	var dBVehicle interface{}

	var data map[string]interface{}
	err = json.Unmarshal(cloudEvent.Data, &data)
	if err != nil {
		cs.logger.Err(err).Msg("Failed to unmarshal JSON")
		return err
	}
	// Extract the VIN field
	vin, ok := data["vin"].(string)
	if !ok {
		return fmt.Errorf("VIN is missing in the message data for CloudEvent ID: %s", cloudEvent.ID)
	}

	// Validate signals
	err = validateSignals(data, vin, cs.logger)
	if err != nil {
		return err
	}

	vehicleID := vin
	cachedResponse, found := cs.cache.Get(vehicleID)
	if found {
		cs.logger.Debug().Msgf("Cache hit for vehicleID: %s", vehicleID)
		dBVehicle = cachedResponse
	} else {
		cs.logger.Debug().Msgf("Cache miss for vehicleID: %s", vehicleID)
		response, err := cs.Db.GetVehicleByVin(cs.Ctx, vehicleID)
		if err != nil {
			failedStatusEventCntr.Inc()
			cs.logger.Error().Err(err).Msgf("Error querying vehicle by vehicleID: %s", vehicleID)
			return err
		}
		dBVehicle = response
		cs.cache.Set(vehicleID, response, cache.DefaultExpiration)
	}
	vehicle := dBVehicle.(*dbmodels.Vin)

	if vehicle != nil && vehicle.VehicleTokenID.Int64 == 0 {
		cs.logger.Debug().Msgf("Vehicle token ID is 0 for VIN: %s , do not send to DIS", vehicle.Vin)
		return nil
	}

	// Set the producer DID and subject for the CloudEvent
	err = convert.SetProducerAndSubject(*vehicle, cloudEvent, cs.settings)
	if err != nil {
		return err
	}

	// Send the DISEvent to the Dimo Node
	return cs.HandleSendToDIS(cloudEvent)
}

func (cs *OracleService) HandleSendToDIS(ce *cloudevent.CloudEvent[json.RawMessage]) error {
	// Send the CloudEvent to the Dimo Node
	statusCode, err := cs.dimoNodeAPISvc.SendToDimoNode(ce)
	if err != nil {
		failedStatusEventCntr.Inc()
		cs.logger.Error().Err(err).Msg("Failed to send event to Dimo Node")
		return err
	}

	if statusCode == http.StatusBadRequest {
		failedStatusEventCntr.Inc()
		// Just log it and do not retry
		cs.logger.Error().Err(err).Msg("Failed to send event to Dimo Node")
		return nil
	}

	successStatusEventCntr.Inc()
	cs.logger.Debug().Msg("Successfully sent event to Dimo Node")
	return nil
}

func validateSignals(data map[string]interface{}, vin string, logger zerolog.Logger) error {
	signals, ok := data["signals"]
	if !ok {
		return fmt.Errorf("signals are missing in the message data for VIN: %s", vin)
	}

	err := convert.ValidateSignals(signals, logger)
	if err != nil {
		logger.Err(err).Msg("Failed to validate signals in CloudEvent")
		return err
	}
	return nil
}

// Prometheus metrics
var successStatusEventCntr = promauto.NewCounter(prometheus.CounterOpts{
	Name: "oracle_example_success_status_event_total",
	Help: "Total success events processed",
})

var failedStatusEventCntr = promauto.NewCounter(prometheus.CounterOpts{
	Name: "oracle_example_failed_status_events_total",
	Help: "Total number of failed events",
})
