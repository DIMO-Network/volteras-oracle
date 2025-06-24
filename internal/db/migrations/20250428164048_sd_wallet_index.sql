-- +goose Up
-- +goose StatementBegin
SELECT 'up SQL query';

create sequence oracle_example.sd_wallet_index_seq
    start with 1;

select setval('oracle_example.sd_wallet_index_seq', (select coalesce(max(oracle_example.vins.wallet_index), 0) + 1 from oracle_example.vins));
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
SELECT 'down SQL query';

drop sequence oracle_example.sd_wallet_index_seq;

-- +goose StatementEnd
