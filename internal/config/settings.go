package config

import (
	"github.com/DIMO-Network/shared/pkg/db"
	"github.com/ethereum/go-ethereum/common"
	"net/url"
)

// Settings contains the application config
type Settings struct {
	Environment    string      `yaml:"ENVIRONMENT"`
	LogLevel       string      `yaml:"LOG_LEVEL"`
	Port           string      `yaml:"PORT"`
	MonitoringPort string      `yaml:"MONITORING_PORT"`
	DB             db.Settings `yaml:"DB"`              // should be secrets
	JwtKeySetURL   string      `yaml:"JWT_KEY_SET_URL"` // DIMO JWT key set.

	// Just an example - Communication and Auth with your external system. Should all be secrets
	ExternalVendorAPIURL string `yaml:"EXTERNAL_VENDOR_APIURL"` // your system's api url
	ClientID             string `yaml:"CLIENT_ID"`              // auth client id example
	ClientSecret         string `yaml:"CLIENT_SECRET"`          // auth secret example
	Audience             string `yaml:"AUDIENCE"`               // some other parameter you may need

	// Kafka - in this example we stream from kafka
	IsTelemetryConsumerEnabled  bool   `yaml:"IS_TELEMETRY_CONSUMER_ENABLED"`
	IsOperationsConsumerEnabled bool   `yaml:"IS_OPERATIONS_CONSUMER_ENABLED"`
	KafkaBrokers                string `yaml:"KAFKA_BROKERS"` // should be secrets

	// Kafka - Operations topic - in this example, enrollment status is communicated async via kafka
	OperationsTopic         string `yaml:"OPERATIONS_TOPIC"`
	OperationsConsumerGroup string `yaml:"OPERATIONS_CONSUMER_GROUP"`

	// Kafka - Unbuffered Telemetry topic - in this example, raw vehicle telemetry is streamed via kafka
	UnbufferedTelemetryTopic         string `yaml:"UNBUFFERED_TELEMETRY_TOPIC"`
	UnbufferedTelemetryConsumerGroup string `yaml:"UNBUFFERED_TELEMETRY_CONSUMER_GROUP"`

	// DIS - DIMO Ingest Service
	DimoNodeEndpoint string `yaml:"DIMO_NODE_ENDPOINT"`
	Cert             string `yaml:"CERT"`     // should be secrets
	CertKey          string `yaml:"CERT_KEY"` // should be secrets
	CACert           string `yaml:"CA_CERT"`  // DIMO Root CA, same for everybody

	// Chain - These are standard Polygon values for DIMO
	ChainID             int64          `yaml:"CHAIN_ID"`
	VehicleNftAddress   common.Address `yaml:"VEHICLE_NFT_ADDRESS"`
	SyntheticNftAddress common.Address `yaml:"SYNTHETIC_NFT_ADDRESS"`

	// Identity-api - DIMO Identity Service api
	IdentityAPIEndpoint url.URL `yaml:"IDENTITY_API_ENDPOINT"`

	// Device-Definitions API - DIMO Device Definitions Service api - used for Decoding VINs
	DeviceDefinitionsAPIEndpoint url.URL `yaml:"DEVICE_DEFINITIONS_API_ENDPOINT"`

	// Transactions SDK
	DeveloperAAWalletAddress common.Address `yaml:"DEVELOPER_AA_WALLET_ADDRESS"` // should be secret - dimo can generate for you
	DeveloperPK              string         `yaml:"DEVELOPER_PK"`                // should be secret (private key) - used for signing transactions
	RPCURL                   url.URL        `yaml:"RPC_URL"`                     // eg alchemy URL, secret since it contains your API Key
	BundlerURL               url.URL        `yaml:"BUNDLER_URL"`                 // eg. zerodev, secret since it contains your API Key
	RegistryAddress          common.Address `yaml:"REGISTRY_ADDRESS"`            // standard Polygon registry address for DIMO

	// DIMO Auth - uses your dev console client ID and secret to authenticate with DIMO Auth to get JWT's for authenticated API calls.
	DimoAuthURL        url.URL        `yaml:"DIMO_AUTH_URL"`
	DimoAuthClientID   common.Address `yaml:"DIMO_AUTH_CLIENT_ID"`
	DimoAuthDomain     url.URL        `yaml:"DIMO_AUTH_DOMAIN"`
	DimoAuthPrivateKey string         `yaml:"DIMO_AUTH_PRIVATE_KEY"` // should be secret

	// SD Wallats
	SDWalletsSeed string `yaml:"SD_WALLETS_SEED"` // should be secret, used for minting Synthetic Devices

	// Minting
	EnableMintingWithConnectionTokenID bool   `yaml:"ENABLE_MINTING_WITH_CONNECTION_TOKEN_ID"`
	ConnectionTokenID                  string `yaml:"CONNECTION_TOKEN_ID"`

	// Onboarding - can be useful to disable this for local testing / debugging
	EnableVendorCapabilityCheck bool `yaml:"ENABLE_VENDOR_CAPABILITY_CHECK"`
	EnableVendorConnection      bool `yaml:"ENABLE_VENDOR_CONNECTION"`
	EnableVendorTestMode        bool `yaml:"ENABLE_VENDOR_TEST_MODE"`
}

func (s *Settings) IsProduction() bool {
	return s.Environment == "prod" // this string is set in the helm chart values-prod.yaml
}
