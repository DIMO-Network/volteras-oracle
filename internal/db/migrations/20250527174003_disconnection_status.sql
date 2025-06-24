-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

alter table oracle_example.vins
    add disconnection_status varchar(30);

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

alter table oracle_example.vins
drop column disconnection_status;

-- +goose StatementEnd
