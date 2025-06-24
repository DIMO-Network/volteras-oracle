-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

alter table oracle_example.vins
    add onboarding_status integer not null default 0;

alter table oracle_example.vins
    add device_definition_id text;

alter table oracle_example.vins
    add wallet_index bigint;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

alter table oracle_example.vins
drop column onboarding_status;

alter table oracle_example.vins
drop column device_definition_id;

alter table oracle_example.vins
drop column wallet_index;

-- +goose StatementEnd
