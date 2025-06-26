package service

import "github.com/DIMO-Network/volteras-oracle/internal/config"

//go:generate mockgen -source volteras_api.go -destination mocks/volteras_api_mock.go -package mocks
type VolterasAPI interface {
	GetAuthToken() (string, error)
}

type volterasAPI struct {
	settings *config.Settings
}

func NewVolterasAPI(settings config.Settings) VolterasAPI {
	return &volterasAPI{
		settings: &settings,
	}
}

func (v *volterasAPI) GetAuthToken() (string, error) {
	return "", nil
}
