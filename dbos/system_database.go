package dbos

import (
	"context"
	"embed"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type SystemDatabase interface {
	Destroy() error
	InsertWorkflowStatus(ctx context.Context, initStatus WorkflowStatus) (*InsertWorkflowResult, error)
}

type systemDatabase struct {
	pool *pgxpool.Pool
}

/*******************************/
/******* INITIALIZATION ********/
/*******************************/

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

//go:embed migrations/*.sql
var migrationFiles embed.FS

// TODO: must use the systemdb name
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

func (s *systemDatabase) Destroy() error {
	s.pool.Close()
	return nil
}

/*******************************/
/******* STATUS MANAGEMENT ********/
/*******************************/

type InsertWorkflowResult struct {
	Attempts                int    `json:"recovery_attempts"`
	Status                  string `json:"status"`
	Name                    string `json:"name"`
	ClassName               string `json:"class_name"`
	ConfigName              string `json:"config_name"`
	QueueName               string `json:"queue_name"`
	WorkflowDeadlineEpochMs *int64 `json:"workflow_deadline_epoch_ms"`
}

func (s *systemDatabase) InsertWorkflowStatus(ctx context.Context, initStatus WorkflowStatus) (*InsertWorkflowResult, error) {
	// Set default values
	attempts := 1
	if initStatus.Status == "ENQUEUED" {
		attempts = 0
	}

	updatedAt := time.Now()
	if !initStatus.UpdatedAt.IsZero() {
		updatedAt = initStatus.UpdatedAt
	}

	var deadline *int64 = nil
	if !initStatus.Deadline.IsZero() {
		millis := initStatus.Deadline.UnixMilli()
		deadline = &millis
	}

	query := `INSERT INTO dbos.workflow_status (
        workflow_uuid,
        status,
        name,
        class_name,
        config_name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        executor_id,
        application_version,
        application_id,
        created_at,
        recovery_attempts,
        updated_at,
        workflow_timeout_ms,
        workflow_deadline_epoch_ms,
        inputs,
        deduplication_id,
        priority
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
            recovery_attempts = workflow_status.recovery_attempts + 1,
            updated_at = EXCLUDED.updated_at,
            executor_id = EXCLUDED.executor_id
        RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms`

	var result InsertWorkflowResult
	err := s.pool.QueryRow(ctx, query,
		initStatus.ID,
		initStatus.Status,
		initStatus.Name,
		"", // WorkflowClassName is not used in the query, so we pass an empty string
		"", // WorkflowConfigName is not used in the query, so we pass an empty string
		"", // initStatus.QueueName,
		"", // initStatus.AuthenticatedUser,
		"", // initStatus.AssumedRole,
		"", // string(authenticatedRolesJSON),
		initStatus.ExecutorID,
		initStatus.ApplicationVersion,
		initStatus.ApplicationID,
		initStatus.CreatedAt.UnixMilli(),
		attempts,
		updatedAt.UnixMilli(),
		initStatus.Timeout.Milliseconds(),
		deadline,
		initStatus.Input,
		"", // initStatus.DeduplicationID,
		1,  // initStatus.Priority,
	).Scan(
		&result.Attempts,
		&result.Status,
		&result.Name,
		&result.ClassName,
		&result.ConfigName,
		&result.QueueName,
		&result.WorkflowDeadlineEpochMs, // We should convert this to a time.Time
	)

	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	return &result, nil
}
