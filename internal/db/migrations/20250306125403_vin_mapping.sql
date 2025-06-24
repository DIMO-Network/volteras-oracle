-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';
CREATE TABLE oracle_example.vins
(
    vin                VARCHAR(17)
        CONSTRAINT vins_pk
            PRIMARY KEY,
    vehicle_token_id   BIGINT
        CONSTRAINT unique_vehicle_token_id UNIQUE,
    synthetic_token_id BIGINT
        CONSTRAINT unique_synthetic_token_id UNIQUE,
    external_id        VARCHAR(255),
    connection_status  VARCHAR(30)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';
DROP TABLE oracle_example.vins;
-- +goose StatementEnd
