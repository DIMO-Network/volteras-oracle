package service

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"github.com/DIMO-Network/cloudevent"
	shttp "github.com/DIMO-Network/shared/pkg/http"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/rs/zerolog"
	"golang.org/x/time/rate"
	"io"
	"net/http"
	"time"
)

type DimoNodeAPI interface {
	SendToDimoNode(event *cloudevent.CloudEvent[json.RawMessage]) (interface{}, error)
}

type dimoNodeService struct {
	httpClient shttp.ClientWrapper
	apiURL     string
	logger     zerolog.Logger
}

func NewDimoNodeAPIService(logger zerolog.Logger, settings config.Settings) DimoNodeAPI {
	h := map[string]string{}
	h["Content-Type"] = "application/json"
	transport, err := initTLSClient(logger, settings)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to initialize TLS client")
	}

	hcw, _ := shttp.NewClientWrapper("", "", 10*time.Second, h, false,
		shttp.WithRetry(3), shttp.WithTransport(transport), shttp.WithLimiter(rate.NewLimiter(50, 100)))
	return &dimoNodeService{
		httpClient: hcw,
		apiURL:     settings.DimoNodeEndpoint,
		logger:     logger,
	}
}

// initTLSClient initializes a TLS client with the provided settings and returns an HTTP transport.
// It loads the client certificate, private key, and CA certificate, and sets up the TLS configuration.
func initTLSClient(logger zerolog.Logger, settings config.Settings) (*http.Transport, error) {
	// Load client certificate and private key
	clientCertBytes := []byte(settings.Cert)
	privateKeyBytes := []byte(settings.CertKey)
	cert, err := tls.X509KeyPair(clientCertBytes, privateKeyBytes)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load client certificate and private key")
	}

	// Load the CA certificate
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to read CA certificate")
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM([]byte(settings.CACert))

	// Set up HTTPS client with the loaded certificates
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
	}

	transport := &http.Transport{TLSClientConfig: tlsConfig}
	return transport, nil
}

// SendToDimoNode sends a CloudEvent to the Dimo Node API.
func (d *dimoNodeService) SendToDimoNode(event *cloudevent.CloudEvent[json.RawMessage]) (interface{}, error) {
	// Marshal the event into JSON
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		d.logger.Err(err).Msg("Failed to marshal event")
		return "", err
	}

	d.logger.Debug().Msgf("Sending request to Dimo Node : %s", d.apiURL)
	resp, err := d.httpClient.ExecuteRequest(d.apiURL, http.MethodPost, payloadBytes)
	if err != nil {
		// HTTPClientWrapper treats all 4xx status codes as errors, so we need to handle them here
		d.logger.Err(err).Msg("Failed to send POST request")

		// Handle 401 Unauthorized and 403 Forbidden status codes
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			err401 := fmt.Errorf("received status code: %d", resp.StatusCode)
			d.logger.Err(err).Msg("Unauthorized or Forbidden error")
			return resp.StatusCode, err401
		}

		// Handle 404 Not Found status code
		if resp.StatusCode == http.StatusNotFound {
			err404 := fmt.Errorf("received status code: %d", resp.StatusCode)
			d.logger.Err(err).Msg("Not Found error")
			return resp.StatusCode, err404
		}

		// Handle 5xx status codes
		if resp.StatusCode >= 500 && resp.StatusCode < 600 {
			err5XX := fmt.Errorf("received 5xx status code: %d", resp.StatusCode)
			d.logger.Err(err).Msg("Server error")
			return resp.StatusCode, err5XX
		}

		// Handle 400 Bad Request status code
		// Just log it and do not retry
		if resp.StatusCode == http.StatusBadRequest {
			d.logger.Warn().Msgf("received status code: %d", resp.StatusCode)
			return resp.StatusCode, nil
		}

		return "", err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			d.logger.Err(err).Msg("Failed to close response body")
		}
	}(resp.Body)

	// Print the HTTP response status code
	d.logger.Debug().Msgf("Received HTTP response status code: %d", resp.StatusCode)

	return resp.StatusCode, nil
}
