-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

UPDATE oracle_example.vins
SET onboarding_status = 53
WHERE onboarding_status = 93;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

UPDATE oracle_example.vins
SET onboarding_status = 53
WHERE onboarding_status = 93;

-- +goose StatementEnd
