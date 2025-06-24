package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/DIMO-Network/shared/pkg/db"
	"github.com/DIMO-Network/volteras-oracle/internal/config"
	"github.com/google/subcommands"
	_ "github.com/lib/pq" // required for this to work
	"github.com/pressly/goose/v3"
	"github.com/rs/zerolog"
)

type migrateDBCmd struct {
	logger   zerolog.Logger
	settings config.Settings
	pdb      db.Store

	up   bool
	down bool
}

func (*migrateDBCmd) Name() string     { return "migrate" }
func (*migrateDBCmd) Synopsis() string { return "migrate database to latest version" }
func (*migrateDBCmd) Usage() string {
	return `migrate [-up | -down]:
	migrates database up or down accordingly. No argument default is up.
  `
}

func (p *migrateDBCmd) SetFlags(f *flag.FlagSet) {
	f.BoolVar(&p.up, "up", false, "up database")
	f.BoolVar(&p.down, "down", false, "down database")
}

const migrationsDir = "internal/db/migrations"

func (p *migrateDBCmd) Execute(ctx context.Context, _ *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	dbInst := p.pdb.DBS().Writer
	if err := dbInst.Ping(); err != nil {
		p.logger.Fatal().Msgf("failed to ping db: %v\n", err)
	}
	// set default
	command := "up"
	if p.down {
		command = "down"
	}
	fmt.Printf("migrate command received is: %s \n", command)

	// manually run sql to create schema
	_, err := dbInst.Exec(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s;", p.settings.DB.Name))
	if err != nil {
		fmt.Println("could not create schema: ", err.Error())
		p.logger.Fatal().Err(err).Msg("could not create schema")
	}
	// set the table to use the same schema
	goose.SetTableName(p.settings.DB.Name + ".migrations")
	if err := goose.RunContext(ctx, command, dbInst.DB, migrationsDir); err != nil {
		fmt.Println("failed to apply go code migrations: ", err.Error())
		p.logger.Fatal().Msgf("failed to apply go code migrations: %v\n", err)
	}
	// if we add any code migrations import _ "github.com/DIMO-Network/volteras-oracle/migrations" // migrations won't work without this

	return subcommands.ExitSuccess
}
