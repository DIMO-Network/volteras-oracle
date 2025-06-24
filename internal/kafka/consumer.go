package kafka

import (
	"context"
	"encoding/json"
	"github.com/DIMO-Network/volteras-oracle/internal/models"
	"github.com/DIMO-Network/volteras-oracle/internal/service"
	"github.com/IBM/sarama"
	"github.com/rs/zerolog"
	"time"
)

// MessageHandlerUnbuffered is a Kafka consumer group handler for unbuffered telemetry messages.
type MessageHandlerUnbuffered struct {
	Logger        *zerolog.Logger
	OracleService *service.OracleService
}

func (h MessageHandlerUnbuffered) Setup(_ sarama.ConsumerGroupSession) error {
	h.Logger.Info().Msg("UnbufferedTelemetryTopic partition allocation started.")
	return nil
}

func (h MessageHandlerUnbuffered) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.Logger.Info().Msg("UnbufferedTelemetryTopic consumer group cleanup initiated.")
	return nil
}

// MessageHandlerOperations is a Kafka consumer group handler for operations messages.
type MessageHandlerOperations struct {
	Logger            *zerolog.Logger
	OracleService     *service.OracleService
	EnrollmentChannel chan models.OperationMessage
}

func (h MessageHandlerOperations) Setup(_ sarama.ConsumerGroupSession) error {
	h.Logger.Info().Msg("OperationsTopic partition allocation started.")
	return nil
}

func (h MessageHandlerOperations) Cleanup(_ sarama.ConsumerGroupSession) error {
	h.Logger.Info().Msg("OperationsTopic consumer group cleanup initiated.")
	return nil
}

func SetupKafkaConsumer(
	ctx context.Context,
	logger *zerolog.Logger,
	brokerList []string,
	topic string,
	consumerGroupID string,
	handler sarama.ConsumerGroupHandler,
) error {
	logger.Info().
		Strs("brokers", brokerList).
		Str("topic", topic).
		Str("consumerGroup", consumerGroupID).
		Msg("Setting up Sarama Kafka consumer.")

	// Create Sarama consumer group
	consumer, err := sarama.NewConsumerGroup(brokerList, consumerGroupID, getSaramaConfig())
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to create Sarama consumer group")
		return err
	}

	logger.Info().Msgf("Sarama consumer group %s created successfully.", consumerGroupID)

	// Context for consumer
	go func() {
		for {
			err := consumer.Consume(ctx, []string{topic}, handler)
			if err != nil {
				logger.Error().Err(err).Msg("Error consuming messages from Kafka")
				return
			}

			if ctx.Err() != nil {
				// Exit loop if context is canceled
				return
			}
		}
	}()

	// Handle graceful shutdown
	go func() {
		<-ctx.Done()
		logger.Info().Msg("Shutting down Sarama consumer group...")
		if err := consumer.Close(); err != nil {
			logger.Error().Err(err).Msg("Error closing Sarama consumer group")
		}
	}()

	return nil
}

func getSaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Net.DialTimeout = 10 * time.Second
	config.Version = sarama.V1_0_0_0
	return config
}

func (h MessageHandlerUnbuffered) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		h.Logger.Debug().
			Str("topic", msg.Topic).
			Int32("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Str("key", string(msg.Key)).
			Str("value", string(msg.Value)).
			Msg("Received Kafka message for UnbufferedTelemetryTopic")

		// Process the message using OracleService
		err := h.OracleService.HandleDeviceByVIN(msg.Value)
		if err != nil {
			h.Logger.Error().Err(err).Msg("Failed to process Kafka message")
			continue
		}

		// Mark the message as processed
		s.MarkMessage(msg, "")
	}
	return nil
}

const OperationTypeEnrollment = "enrollment"

const (
	ActionEnroll   = "enroll"
	ActionUnenroll = "unenroll"
)

const (
	OperationStatusInQueue    string = "inQueue"
	OperationStatusInProgress string = "inProgress"
	OperationStatusSucceeded  string = "succeeded"
	OperationStatusFailed     string = "failed"
)

func (h MessageHandlerOperations) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		h.Logger.Debug().
			Str("topic", msg.Topic).
			Int32("partition", msg.Partition).
			Int64("offset", msg.Offset).
			Str("key", string(msg.Key)).
			Str("value", string(msg.Value)).
			Msg("Received Kafka message for OperationsTopic")

		// Parse the Kafka message into OperationMessage
		var operation models.OperationMessage
		if err := json.Unmarshal(msg.Value, &operation); err != nil {
			h.Logger.Error().Err(err).Msg("Failed to parse Kafka message into OperationMessage")
			continue
		}

		h.Logger.Debug().Interface("operation", operation).Msg("Received Kafka message for OperationsTopic")

		if operation.Type == OperationTypeEnrollment {
			var operationError *models.OperationError

			if operation.Action == ActionEnroll {
				if operation.Status == OperationStatusFailed {
					operationError = &operation.Error
				}

				var vehicleId string
				vehicleId = operation.ID // We let external_id be operation.ID until we get the vehicleId from the motorq
				// When operation succeeds, update the database with VIN, Status, and vehicle_Id
				if operation.Status == OperationStatusSucceeded {
					vehicleId = operation.Data.VehicleID
				}

				h.EnrollmentChannel <- operation

				// Update the database with VIN, Status, and vehicleId
				if err := h.OracleService.Db.UpdateEnrollmentStatus(h.OracleService.Ctx, operation.VIN, operation.Status, vehicleId, operationError); err != nil {
					h.Logger.Error().Err(err).Msgf("Failed to update database for VIN: %s", operation.VIN)
					continue
				}

				h.Logger.Debug().Msgf("Successfully updated database for VIN: %s with Status: %s and externalId: %s", operation.VIN, operation.Status, vehicleId)
			} else if operation.Action == ActionUnenroll {
				h.EnrollmentChannel <- operation

				// Update the database with VIN, Status
				if err := h.OracleService.Db.UpdateUnenrollmentStatus(h.OracleService.Ctx, operation.VIN, operation.Status, operationError); err != nil {
					h.Logger.Error().Err(err).Msgf("Failed to update database for VIN: %s", operation.VIN)
					continue
				}

				h.Logger.Debug().Msgf("Successfully updated database for VIN: %s with Status: %s and externalId: %s", operation.VIN, operation.Status, operation.ID)

			}

		}
		// Mark the message as processed
		s.MarkMessage(msg, "")
	}
	return nil
}
