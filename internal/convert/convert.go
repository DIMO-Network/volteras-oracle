package convert

import (
	"encoding/json"
	"fmt"
	"github.com/DIMO-Network/cloudevent"
	"github.com/DIMO-Network/model-garage/pkg/defaultmodule"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	dbmodels "github.com/DIMO-Network/volteras-oracle/internal/db/models"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/rs/zerolog"
	"time"
)

// SetProducerAndSubject sets the producer and consumer fields in the CloudEvent
func SetProducerAndSubject(veh dbmodels.Vin, ce *cloudevent.CloudEvent[json.RawMessage], settings config.Settings) error {
	// Construct the producer DID
	producer := cloudevent.NFTDID{
		ChainID:         uint64(settings.ChainID),
		ContractAddress: settings.SyntheticNftAddress,
		TokenID:         uint32(veh.SyntheticTokenID.Int64),
	}.String()

	// Construct the subject
	var subject string
	vehTokenId := uint32(veh.VehicleTokenID.Int64)
	if vehTokenId != 0 {
		subject = cloudevent.NFTDID{
			ChainID:         uint64(settings.ChainID),
			ContractAddress: settings.VehicleNftAddress,
			TokenID:         vehTokenId,
		}.String()
	}

	ce.Subject = subject
	ce.Producer = producer

	return nil
}

// ValidateSignals validates the signals from the message
func ValidateSignals(signals interface{}, logger zerolog.Logger) error {

	signalsArr, ok := signals.([]interface{})
	if !ok {
		err := fmt.Errorf("signals is not of type []interface{}")
		logger.Error().Err(err).Msg("Invalid type for signals")
		return err
	}

	signalArray, err := CastToSliceOfMaps(signalsArr)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to cast signals to slice of maps")
		return err
	}

	// Load the signal map
	sigMap, err := defaultmodule.LoadSignalMap()
	if err != nil {
		return err
	}

	for _, signal := range signalArray {
		name, ok := signal["name"].(string)
		if !ok {
			logger.Warn().Msgf("Signal name is missing or not a string: %v\n", signal)
			continue
		}

		if _, exists := sigMap[name]; !exists {
			logger.Warn().Msgf("Signal %s is not in the signal map\n", name)
			continue
		}
	}

	return nil
}

// MapDataToSignals maps the data from the message to the default DIS signals
func MapDataToSignals(data models.Data, ts time.Time) ([]*defaultmodule.Signal, error) {
	var signals []*defaultmodule.Signal

	sigMap, err := defaultmodule.LoadSignalMap()
	if err != nil {
		return nil, err
	}

	if _, exists := sigMap["speed"]; exists {
		speedSignal := &defaultmodule.Signal{
			Name:      "speed",
			Timestamp: ts,
			Value:     data.Speed.Value,
		}
		signals = append(signals, speedSignal)
	}

	if _, exists := sigMap["powertrainFuelSystemRelativeLevel"]; exists && data.FuelLevel.Value > 0 && data.FuelLevel.Value <= 100 {
		fuelSignal := &defaultmodule.Signal{
			Name:      "powertrainFuelSystemRelativeLevel",
			Timestamp: ts,
			Value:     data.FuelLevel.Value,
		}
		signals = append(signals, fuelSignal)
	}

	if _, exists := sigMap["powertrainTransmissionTravelledDistance"]; exists {
		odometerSignal := &defaultmodule.Signal{
			Name:      "powertrainTransmissionTravelledDistance",
			Timestamp: ts,
			Value:     milesToKilometers(data.Odometer.Value),
		}
		signals = append(signals, odometerSignal)
	}

	if _, exists := sigMap["currentLocationLongitude"]; exists && data.Location.Lon != 0 {
		odometerSignal := &defaultmodule.Signal{
			Name:      "currentLocationLongitude",
			Timestamp: ts,
			Value:     data.Location.Lon,
		}
		signals = append(signals, odometerSignal)
	}

	if _, exists := sigMap["currentLocationLatitude"]; exists && data.Location.Lat != 0 {
		odometerSignal := &defaultmodule.Signal{
			Name:      "currentLocationLatitude",
			Timestamp: ts,
			Value:     data.Location.Lat,
		}
		signals = append(signals, odometerSignal)
	}

	return signals, nil
}

func CastToSliceOfMaps(signals []interface{}) ([]map[string]interface{}, error) {
	var result []map[string]interface{}

	for _, item := range signals {
		castItem, ok := item.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("failed to cast item to map[string]interface{}: %v", item)
		}
		result = append(result, castItem)
	}

	return result, nil
}
