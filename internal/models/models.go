package models

type GraphQLRequest struct {
	Query string `json:"query"`
}

type Vehicle struct {
	VIN                 string          `json:"vin"`
	ID                  string          `json:"id"`
	TokenID             int64           `json:"tokenId"`
	MintedAt            string          `json:"mintedAt"`
	Owner               string          `json:"owner"`
	Definition          Definition      `json:"definition"`
	SyntheticDevice     SyntheticDevice `json:"syntheticDevice"`
	ConnectionStatus    string          `json:"connectionStatus"`
	DisconnectionStatus string          `json:"disconnectionStatus"`
}

type SyntheticDevice struct {
	ID       string `json:"id"`
	TokenID  int64  `json:"tokenId"`
	MintedAt string `json:"mintedAt"`
}

type Definition struct {
	ID    string `json:"id"`
	Make  string `json:"make"`
	Model string `json:"model"`
	Year  int    `json:"year"`
}

type SingleVehicle struct {
	Vehicle Vehicle `json:"vehicle"`
}

type SingleVehicleData struct {
	Data SingleVehicle `json:"data"`
}

type PageInfo struct {
	HasPreviousPage bool   `json:"hasPreviousPage"`
	HasNextPage     bool   `json:"hasNextPage"`
	StartCursor     string `json:"startCursor"`
	EndCursor       string `json:"endCursor"`
}

type PagedVehiclesNodes struct {
	Nodes    []Vehicle `json:"nodes"`
	PageInfo PageInfo  `json:"pageInfo"`
}

type PagedVehicles struct {
	VehicleNodes PagedVehiclesNodes `json:"vehicles"`
}

type SingleDeviceDefinition struct {
	DeviceDefinition DeviceDefinition `json:"deviceDefinition"`
}

type DeviceDefinition struct {
	DeviceDefinitionID string       `json:"deviceDefinitionId"`
	Manufacturer       Manufacturer `json:"manufacturer"`
	Model              string       `json:"model"`
	Year               int          `json:"year"`
}

type Manufacturer struct {
	TokenID uint64 `json:"tokenId"`
	Name    string `json:"name"`
}

type GraphQlData[T any] struct {
	Data T `json:"data"`
}

type Data struct {
	Location  Location `json:"location"`
	Speed     Signal   `json:"speed"`
	Odometer  Signal   `json:"odometer"`
	FuelLevel Signal   `json:"fuelLevel"`
}

type Location struct {
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}
type Signal struct {
	Value      float64 `json:"value"`
	SignalType string  `json:"signalType"`
	Units      string  `json:"units"`
}

type SubStatus struct {
	SourceTelemetryFuel struct {
		Status string `json:"status"`
	} `json:"source.telemetryFuel"`
}

type OperationError struct {
	Code        string `json:"code"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

type OperationMessage struct {
	ID               string         `json:"id"`
	Type             string         `json:"type"`
	Action           string         `json:"action"`
	CreatedTimestamp string         `json:"createdTimestamp"`
	Status           string         `json:"status"`
	VIN              string         `json:"vin"`
	DataType         string         `json:"dataType"`
	Error            OperationError `json:"error"`
	SubStatus        SubStatus      `json:"subStatus"`
	Data             struct {
		ID               string   `json:"id"`
		CreatedTimestamp string   `json:"createdTimestamp"`
		VehicleID        string   `json:"vehicleId"`
		DataSource       string   `json:"dataSource"`
		Account          string   `json:"account"`
		VIN              string   `json:"vin"`
		DataServices     []string `json:"dataServices"`
		SerialNumber     *string  `json:"serialNumber"`
	} `json:"data"`
}
