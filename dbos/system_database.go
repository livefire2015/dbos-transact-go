package dbos

import (
	"context"
	"embed"
	"fmt"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type SystemDatabase interface {
	Close() error
}

type systemDatabase struct {
	pool *pgxpool.Pool
}

// createDatabaseIfNotExists creates the database if it doesn't exist
func createDatabaseIfNotExists(databaseURL string) error {
	// Connect to the postgres database
	parsedURL, err := pgx.ParseConfig(databaseURL)
	if err != nil {
		return fmt.Errorf("failed to parse database URL: %w", err)
	}

	dbName := parsedURL.Database
	if dbName == "" {
		return fmt.Errorf("database name not found in URL")
	}

	serverURL := *parsedURL
	serverURL.Database = "postgres"
	conn, err := pgx.ConnectConfig(context.Background(), &serverURL)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL server: %w", err)
	}
	defer conn.Close(context.Background())

	// Create the system database if it doesn't exist
	var exists bool
	err = conn.QueryRow(context.Background(),
		"SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check if database exists: %w", err)
	}
	if !exists {
		// TODO: validate db name
		createSQL := fmt.Sprintf("CREATE DATABASE %s", pgx.Identifier{dbName}.Sanitize())
		_, err = conn.Exec(context.Background(), createSQL)
		if err != nil {
			return fmt.Errorf("failed to create database %s: %w", dbName, err)
		}
	}

	return nil
}

// runMigrations runs the embedded migration files
func runMigrations(databaseURL string) error {
	// Change the driver to pgx5
	databaseURL = "pgx5://" + strings.TrimPrefix(databaseURL, "postgres://")

	// Create migration source from embedded files
	d, err := iofs.New(migrationFiles, "migrations")
	if err != nil {
		return fmt.Errorf("failed to create migration source: %w", err)
	}

	// Create migrator
	m, err := migrate.NewWithSourceInstance("iofs", d, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to create migrator: %w", err)
	}
	defer m.Close()

	// Run migrations
	// FIXME: tolerate errors when the migration is bcz we run an older version of transact
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	return nil
}

// New creates a new SystemDatabase instance and runs migrations
func NewSystemDatabase() (SystemDatabase, error) {
	// TODO: pass proper config
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" {
		return nil, fmt.Errorf("DBOS_DATABASE_URL environment variable is required")
	}

	// Create the database if it doesn't exist
	if err := createDatabaseIfNotExists(databaseURL); err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}

	// Run migrations first
	if err := runMigrations(databaseURL); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	// Create pgx pool
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	// FIXME: remove this
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &systemDatabase{
		pool: pool,
	}, nil
}

func (s *systemDatabase) Close() error {
	s.pool.Close()
	return nil
}
