package mocks

import (
	"errors"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
)

type IdentityAPIMock struct {
	Vehicles          []models.Vehicle
	DeviceDefinitions []models.DeviceDefinition
}

func NewIdentityAPIMock(v []models.Vehicle, dd []models.DeviceDefinition) *IdentityAPIMock {
	return &IdentityAPIMock{
		Vehicles:          v,
		DeviceDefinitions: dd,
	}
}

func (m *IdentityAPIMock) GetVehicleByTokenID(tokenID int64) (*models.Vehicle, error) {
	for _, vehicle := range m.Vehicles {
		if vehicle.TokenID == tokenID {
			return &vehicle, nil
		}
	}

	return nil, errors.New("vehicle not found")
}

func (m *IdentityAPIMock) GetCachedVehicleByTokenID(tokenID int64) (*models.Vehicle, error) {
	for _, vehicle := range m.Vehicles {
		if vehicle.TokenID == tokenID {
			return &vehicle, nil
		}
	}

	return nil, errors.New("vehicle not found")
}

func (m *IdentityAPIMock) FetchVehicleByTokenID(tokenID int64) (*models.Vehicle, error) {
	for _, vehicle := range m.Vehicles {
		if vehicle.TokenID == tokenID {
			return &vehicle, nil
		}
	}

	return nil, errors.New("vehicle not found")
}

func (m *IdentityAPIMock) FetchVehiclesByWalletAddress(address string) ([]models.Vehicle, error) {
	returnVal := []models.Vehicle{}
	for _, vehicle := range m.Vehicles {
		if vehicle.Owner == address {
			returnVal = append(returnVal, vehicle)
		}
	}
	return returnVal, nil
}

func (m *IdentityAPIMock) GetDeviceDefinitionByID(id string) (*models.DeviceDefinition, error) {
	for _, definition := range m.DeviceDefinitions {
		if definition.DeviceDefinitionID == id {
			return &definition, nil
		}
	}

	return nil, errors.New("device definition not found")
}

func (m *IdentityAPIMock) GetCachedDeviceDefinitionByID(id string) (*models.DeviceDefinition, error) {
	for _, definition := range m.DeviceDefinitions {
		if definition.DeviceDefinitionID == id {
			return &definition, nil
		}
	}

	return nil, errors.New("device definition not found")
}

func (m *IdentityAPIMock) FetchDeviceDefinitionByID(id string) (*models.DeviceDefinition, error) {
	for _, definition := range m.DeviceDefinitions {
		if definition.DeviceDefinitionID == id {
			return &definition, nil
		}
	}

	return nil, errors.New("device definition not found")
}
