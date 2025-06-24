-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

alter table oracle_example.vins
    add operation_error_code varchar(30);

alter table oracle_example.vins
    add operation_error_type varchar(30);

alter table oracle_example.vins
    add operation_error_description varchar(512);


-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

alter table oracle_example.vins
    drop column operation_error_code;

alter table oracle_example.vins
    drop column operation_error_type;

alter table oracle_example.vins
    drop column operation_error_description;

-- +goose StatementEnd
