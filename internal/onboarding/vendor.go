package onboarding

import (
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/rs/zerolog"
)

type VendorCapabilityStatus struct {
	VIN    string `json:"vin"`
	Status string `json:"status"`
}

type VendorConnectionStatus struct {
	VIN        string `json:"vin"`
	ExternalID string `json:"externalId"`
	Status     string `json:"status"`
}

type VendorOnboardingAPI interface {
	Validate(vins []string) ([]VendorCapabilityStatus, error)
	Connect(vins []string) ([]VendorConnectionStatus, error)
	Disconnect(vins []string) ([]VendorConnectionStatus, error)
}

type ExternalOnboardingService struct {
	settings          *config.Settings
	db                *service.Vehicle
	logger            *zerolog.Logger
	enrollmentChannel chan models.OperationMessage
}

func NewExternalOnboardingService(settings *config.Settings, db *service.Vehicle, logger *zerolog.Logger, enrollmentChannel chan models.OperationMessage) *ExternalOnboardingService {
	return &ExternalOnboardingService{
		settings:          settings,
		db:                db,
		logger:            logger,
		enrollmentChannel: enrollmentChannel,
	}
}

func (s *ExternalOnboardingService) Validate(vins []string) ([]VendorCapabilityStatus, error) {
	s.logger.Debug().Strs("vins", vins).Msg("vendor.Validate")

	result := make([]VendorCapabilityStatus, 0, len(vins))

	// Here should be any logic / API calls for external vendor system validity check (check if vehicle is capable of vendor connection)

	for _, vin := range vins {
		result = append(result, VendorCapabilityStatus{
			VIN:    vin,
			Status: "capable",
		})
	}

	return result, nil
}

func (s *ExternalOnboardingService) Connect(vins []string) ([]VendorConnectionStatus, error) {
	s.logger.Debug().Strs("vins", vins).Msg("vendor.Connect")

	result := make([]VendorConnectionStatus, 0, len(vins))

	// Here should be any logic / API calls for external vendor system connection

	for _, vin := range vins {
		result = append(result, VendorConnectionStatus{
			VIN:        vin,
			Status:     "succeeded",
			ExternalID: "",
		})
	}

	return result, nil
}

func (s *ExternalOnboardingService) Disconnect(vins []string) ([]VendorConnectionStatus, error) {
	s.logger.Debug().Strs("vins", vins).Msg("vendor.Disconnect")

	result := make([]VendorConnectionStatus, 0, len(vins))

	// Here should be any logic / API calls for external vendor system connection removal

	for _, vin := range vins {
		result = append(result, VendorConnectionStatus{
			VIN:        vin,
			Status:     "succeeded",
			ExternalID: "",
		})
	}

	return result, nil
}
