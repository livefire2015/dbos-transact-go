package dbos

import (
	"bytes"
	"context"
	"embed"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*******************************/
/******* INTERFACE ********/
/*******************************/

type SystemDatabase interface {
	Destroy()
	InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error)
	RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	RecordChildWorkflow(ctx context.Context, input RecordChildWorkflowDBInput) error
	ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error)
	UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error
	AwaitWorkflowResult(ctx context.Context, workflowID string) (any, error)
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

func (s *systemDatabase) Destroy() {
	fmt.Println("Closing system database connection pool")
	s.pool.Close()
}

/*******************************/
/******* STATUS MANAGEMENT ********/
/*******************************/

type InsertWorkflowResult struct {
	Attempts                int                `json:"attempts"`
	Status                  WorkflowStatusType `json:"status"`
	Name                    string             `json:"name"`
	QueueName               *string            `json:"queue_name"`
	WorkflowDeadlineEpochMs *int64             `json:"workflow_deadline_epoch_ms"`
}

type InsertWorkflowStatusDBInput struct {
	Status WorkflowStatus
	Tx     pgx.Tx
}

func (s *systemDatabase) InsertWorkflowStatus(ctx context.Context, input InsertWorkflowStatusDBInput) (*InsertWorkflowResult, error) {
	initStatus := input.Status

	// Set default values
	attempts := 1
	if initStatus.Status == WorkflowStatusEnqueued {
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

	var timeoutMs int64 = 0
	if initStatus.Timeout > 0 {
		timeoutMs = initStatus.Timeout.Milliseconds()
	}

	// Serialize input using gob encoding
	var inputBytes []byte
	if initStatus.Input != nil {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(&initStatus.Input); err != nil {
			return nil, fmt.Errorf("failed to encode input: %w", err)
		}
		inputBytes = buf.Bytes()
	}

	// TODO do not update executor_id when enqueuing a workflow
	query := `INSERT INTO dbos.workflow_status (
        workflow_uuid,
        status,
        name,
        queue_name,
        authenticated_user,
        assumed_role,
        authenticated_roles,
        executor_id,
        application_version,
        application_id,
        created_at,
        attempts,
        updated_at,
        workflow_timeout_ms,
        workflow_deadline_epoch_ms,
        inputs,
        deduplication_id,
        priority
    ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
    ON CONFLICT (workflow_uuid)
        DO UPDATE SET
            attempts = workflow_status.attempts + 1,
            updated_at = EXCLUDED.updated_at,
            executor_id = EXCLUDED.executor_id
        RETURNING attempts, status, name, queue_name, workflow_deadline_epoch_ms`

	var result InsertWorkflowResult
	var err error

	if input.Tx != nil {
		err = input.Tx.QueryRow(ctx, query,
			initStatus.ID,
			initStatus.Status,
			initStatus.Name,
			initStatus.QueueName,
			initStatus.AuthenticatedUser,
			initStatus.AssumedRole,
			initStatus.AuthenticatedRoles,
			initStatus.ExecutorID,
			initStatus.ApplicationVersion,
			initStatus.ApplicationID,
			initStatus.CreatedAt.UnixMilli(),
			attempts,
			updatedAt.UnixMilli(),
			timeoutMs,
			deadline,
			inputBytes,
			initStatus.DeduplicationID,
			initStatus.Priority,
		).Scan(
			&result.Attempts,
			&result.Status,
			&result.Name,
			&result.QueueName,
			&result.WorkflowDeadlineEpochMs,
		)
	} else {
		err = s.pool.QueryRow(ctx, query,
			initStatus.ID,
			initStatus.Status,
			initStatus.Name,
			initStatus.QueueName,
			initStatus.AuthenticatedUser,
			initStatus.AssumedRole,
			initStatus.AuthenticatedRoles,
			initStatus.ExecutorID,
			initStatus.ApplicationVersion,
			initStatus.ApplicationID,
			initStatus.CreatedAt.UnixMilli(),
			attempts,
			updatedAt.UnixMilli(),
			timeoutMs,
			deadline,
			inputBytes,
			initStatus.DeduplicationID,
			initStatus.Priority,
		).Scan(
			&result.Attempts,
			&result.Status,
			&result.Name,
			&result.QueueName,
			&result.WorkflowDeadlineEpochMs,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	return &result, nil
}

type recordOperationResultDBInput struct {
	workflowID    string
	operationID   int
	operationName string
	output        any
	err           error
}

type RecordChildWorkflowDBInput struct {
	ParentWorkflowID string
	ChildWorkflowID  string
	FunctionID       int
	FunctionName     string
	Tx               pgx.Tx
}

func (s *systemDatabase) RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error {
	var query string
	query = `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, output, error, function_name)
            VALUES ($1, $2, $3, $4, $5)
			ON CONFLICT DO NOTHING`

	var errorString *string
	if input.err != nil {
		e := input.err.Error()
		errorString = &e
	}

	var outputBytes []byte
	if input.output != nil {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(&input.output); err != nil {
			return fmt.Errorf("failed to encode output: %w", err)
		}
		outputBytes = buf.Bytes()
	}

	commandTag, err := s.pool.Exec(ctx, query,
		input.workflowID,
		input.operationID,
		outputBytes,
		errorString,
		input.operationName,
	)

	/*
		fmt.Printf("RecordOperationResult - CommandTag: %v\n", commandTag)
		fmt.Printf("RecordOperationResult - Rows affected: %d\n", commandTag.RowsAffected())
		fmt.Printf("RecordOperationResult - SQL: %s\n", commandTag.String())
	*/

	// TODO handle serialization errors
	if err != nil {
		fmt.Printf("RecordOperationResult - Error occurred: %v\n", err)
		return fmt.Errorf("failed to record operation result: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		fmt.Printf("RecordOperationResult - WARNING: No rows were affected by the insert\n")
	}

	return nil
}

func (s *systemDatabase) RecordChildWorkflow(ctx context.Context, input RecordChildWorkflowDBInput) error {
	query := `INSERT INTO dbos.operation_outputs
            (workflow_uuid, function_id, function_name, child_workflow_id)
            VALUES ($1, $2, $3, $4)`

	var commandTag pgconn.CommandTag
	var err error

	if input.Tx != nil {
		commandTag, err = input.Tx.Exec(ctx, query,
			input.ParentWorkflowID,
			input.FunctionID,
			input.FunctionName,
			input.ChildWorkflowID,
		)
	} else {
		commandTag, err = s.pool.Exec(ctx, query,
			input.ParentWorkflowID,
			input.FunctionID,
			input.FunctionName,
			input.ChildWorkflowID,
		)
	}

	if err != nil {
		// Check for unique constraint violation (conflict ID error)
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "23505" {
			return fmt.Errorf("workflow conflict ID error for parent workflow %s", input.ParentWorkflowID)
		}
		return fmt.Errorf("failed to record child workflow: %w", err)
	}

	if commandTag.RowsAffected() == 0 {
		fmt.Printf("RecordChildWorkflow - WARNING: No rows were affected by the insert\n")
	}

	return nil
}

// ListWorkflowsInput represents the input parameters for listing workflows
type ListWorkflowsDBInput struct {
	WorkflowName       *string
	WorkflowIDPrefix   *string
	WorkflowIDs        []string
	AuthenticatedUser  *string
	StartTime          time.Time
	EndTime            time.Time
	Status             *string
	ApplicationVersion *string
	Limit              *int
	Offset             *int
	SortDesc           bool
	Tx                 pgx.Tx
}

// ListWorkflows retrieves a list of workflows based on the provided filters
func (s *systemDatabase) ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error) {
	qb := newQueryBuilder()

	// Build the base query
	baseQuery := `SELECT workflow_uuid, status, name, authenticated_user, assumed_role, authenticated_roles,
	                 output, error, executor_id, created_at, updated_at, application_version, application_id,
	                 attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, started_at_epoch_ms,
					 deduplication_id, inputs, priority
	          FROM dbos.workflow_status`

	// Add filters using query builder
	if input.WorkflowName != nil {
		qb.addWhere("name", *input.WorkflowName)
	}
	if input.WorkflowIDPrefix != nil {
		qb.addWhereLike("workflow_uuid", *input.WorkflowIDPrefix+"%")
	}
	if input.WorkflowIDs != nil && len(input.WorkflowIDs) > 0 {
		qb.addWhereAny("workflow_uuid", input.WorkflowIDs)
	}
	if input.AuthenticatedUser != nil {
		qb.addWhere("authenticated_user", *input.AuthenticatedUser)
	}
	if !input.StartTime.IsZero() {
		qb.addWhereGreaterEqual("created_at", input.StartTime.UnixMilli())
	}
	if !input.EndTime.IsZero() {
		qb.addWhereLessEqual("created_at", input.EndTime.UnixMilli())
	}
	if input.Status != nil {
		qb.addWhere("status", *input.Status)
	}
	if input.ApplicationVersion != nil {
		qb.addWhere("application_version", *input.ApplicationVersion)
	}

	// Build complete query
	var query string
	if len(qb.whereClauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", baseQuery, strings.Join(qb.whereClauses, " AND "))
	} else {
		query = baseQuery
	}

	// Add sorting
	if input.SortDesc {
		query += " ORDER BY created_at DESC"
	} else {
		query += " ORDER BY created_at ASC"
	}

	// Add limit and offset
	if input.Limit != nil {
		qb.argCounter++
		query += fmt.Sprintf(" LIMIT $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Limit)
	}

	if input.Offset != nil {
		qb.argCounter++
		query += fmt.Sprintf(" OFFSET $%d", qb.argCounter)
		qb.args = append(qb.args, *input.Offset)
	}

	// Execute the query
	var rows pgx.Rows
	var err error

	if input.Tx != nil {
		rows, err = input.Tx.Query(ctx, query, qb.args...)
	} else {
		rows, err = s.pool.Query(ctx, query, qb.args...)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to execute ListWorkflows query: %w", err)
	}
	defer rows.Close()

	var workflows []WorkflowStatus
	for rows.Next() {
		var wf WorkflowStatus
		var createdAtMs, updatedAtMs int64
		var timeoutMs *int64
		var deadlineMs, startedAtMs *int64
		var outputBytes, inputBytes []byte
		var errorStr *string

		err := rows.Scan(
			&wf.ID, &wf.Status, &wf.Name, &wf.AuthenticatedUser, &wf.AssumedRole,
			&wf.AuthenticatedRoles, &outputBytes, &errorStr, &wf.ExecutorID, &createdAtMs,
			&updatedAtMs, &wf.ApplicationVersion, &wf.ApplicationID,
			&wf.Attempts, &wf.QueueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &wf.DeduplicationID,
			&inputBytes, &wf.Priority,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan workflow row: %w", err)
		}

		// Convert milliseconds to time.Time
		wf.CreatedAt = time.Unix(0, createdAtMs*int64(time.Millisecond))
		wf.UpdatedAt = time.Unix(0, updatedAtMs*int64(time.Millisecond))

		// Convert timeout milliseconds to time.Duration
		if timeoutMs != nil && *timeoutMs > 0 {
			wf.Timeout = time.Duration(*timeoutMs) * time.Millisecond
		}

		// Convert deadline milliseconds to time.Time
		if deadlineMs != nil {
			wf.Deadline = time.Unix(0, *deadlineMs*int64(time.Millisecond))
		}

		// Convert started at milliseconds to time.Time
		if startedAtMs != nil {
			wf.StartedAt = time.Unix(0, *startedAtMs*int64(time.Millisecond))
		}

		// Convert error string to error type if present
		if errorStr != nil && *errorStr != "" {
			wf.Error = errors.New(*errorStr)
		}

		// Deserialize output from BYTEA using gob
		if outputBytes != nil && len(outputBytes) > 0 {
			buf := bytes.NewBuffer(outputBytes)
			dec := gob.NewDecoder(buf)
			var output any
			if err := dec.Decode(&output); err != nil {
				// If deserialization fails, log the error but continue (don't fail the entire query)
				fmt.Printf("Warning: failed to decode output for workflow %s: %v\n", wf.ID, err)
				wf.Output = nil
			} else {
				wf.Output = output
			}
		}

		// Deserialize input from BYTEA using gob
		if inputBytes != nil && len(inputBytes) > 0 {
			buf := bytes.NewBuffer(inputBytes)
			dec := gob.NewDecoder(buf)
			var input any
			if err := dec.Decode(&input); err != nil {
				// If deserialization fails, log the error but continue (don't fail the entire query)
				fmt.Printf("Warning: failed to decode input for workflow %s: %v\n", wf.ID, err)
				wf.Input = nil
			} else {
				wf.Input = input
			}
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type UpdateWorkflowOutcomeDBInput struct {
	workflowID string
	status     WorkflowStatusType
	output     any
	err        error
	tx         pgx.Tx
}

// Will evolve as we serialize all output and error types
func (s *systemDatabase) UpdateWorkflowOutcome(ctx context.Context, input UpdateWorkflowOutcomeDBInput) error {
	query := `UPDATE dbos.workflow_status
			  SET status = $1, output = $2, error = $3, updated_at = $4, deduplication_id = NULL
			  WHERE workflow_uuid = $5`

	// Serialize output using gob encoding
	var outputBytes []byte
	if input.output != nil {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(&input.output); err != nil {
			return fmt.Errorf("failed to encode output: %w", err)
		}
		outputBytes = buf.Bytes()
	}

	var errorStr string
	if input.err != nil {
		errorStr = input.err.Error()
	}

	var err error
	if input.tx != nil {
		_, err = input.tx.Exec(ctx, query, input.status, outputBytes, errorStr, time.Now().UnixMilli(), input.workflowID)
	} else {
		_, err = s.pool.Exec(ctx, query, input.status, outputBytes, errorStr, time.Now().UnixMilli(), input.workflowID)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}
	return nil
}

func (s *systemDatabase) CancelWorkflow(ctx context.Context, workflowID string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) // Rollback if not committed

	// Check if workflow exists
	listInput := ListWorkflowsDBInput{
		WorkflowIDs: []string{workflowID},
		Tx:          tx,
	}
	wfs, err := s.ListWorkflows(ctx, listInput)
	if err != nil {
		return err
	}
	if len(wfs) == 0 {
		return fmt.Errorf("Workflow %s not found", workflowID)
	}

	wf := wfs[0]
	switch wf.Status {
	case "SUCCESS", "ERROR", "CANCELLED":
		// Workflow is already in a terminal state, rollback and return
		if err := tx.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}

	//Set the workflow's status to CANCELLED
	updateStatusQuery := `UPDATE dbos.workflow_status
						  SET status = 'CANCELLED', updated_at = $1
						  WHERE workflow_uuid = $2`

	_, err = tx.Exec(ctx, updateStatusQuery, time.Now().UnixMilli(), workflowID)
	if err != nil {
		return fmt.Errorf("failed to update workflow status to CANCELLED: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (s *systemDatabase) AwaitWorkflowResult(ctx context.Context, workflowID string) (any, error) {
	query := `SELECT status, output, error FROM dbos.workflow_status WHERE workflow_uuid = $1`
	var status WorkflowStatusType
	for {
		row := s.pool.QueryRow(ctx, query, workflowID)
		var outputBytes []byte
		var errorStr *string
		err := row.Scan(&status, &outputBytes, &errorStr)
		if err != nil {
			if err == pgx.ErrNoRows {
				time.Sleep(1 * time.Second)
				continue
			}
			return nil, fmt.Errorf("failed to query workflow status: %w", err)
		}

		switch status {
		case WorkflowStatusSuccess:
			// Deserialize output from BYTEA using gob
			if outputBytes != nil && len(outputBytes) > 0 {
				buf := bytes.NewBuffer(outputBytes)
				dec := gob.NewDecoder(buf)
				var output any
				if err := dec.Decode(&output); err != nil {
					return nil, fmt.Errorf("failed to decode workflow output: %w", err)
				}
				return output, nil
			}
			return nil, nil // No output
		case WorkflowStatusError:
			return nil, errors.New(*errorStr) // Assuming errorStr can be converted to an error type
		case WorkflowStatusCancelled:
			return nil, fmt.Errorf("workflow %s was cancelled", workflowID)
		default:
			time.Sleep(1 * time.Second) // Wait before checking again
		}
	}
}

/*******************************/
/******* UTILS ********/
/*******************************/

type queryBuilder struct {
	setClauses   []string
	whereClauses []string
	args         []any
	argCounter   int
}

func newQueryBuilder() *queryBuilder {
	return &queryBuilder{
		setClauses:   make([]string, 0),
		whereClauses: make([]string, 0),
		args:         make([]any, 0),
		argCounter:   0,
	}
}

func (qb *queryBuilder) addSet(column string, value any) {
	qb.argCounter++
	qb.setClauses = append(qb.setClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addSetRaw(clause string) {
	qb.setClauses = append(qb.setClauses, clause)
}

func (qb *queryBuilder) addWhere(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s=$%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLike(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s LIKE $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereAny(column string, values any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s = ANY($%d)", column, qb.argCounter))
	qb.args = append(qb.args, values)
}

func (qb *queryBuilder) addWhereGreaterEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s >= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}

func (qb *queryBuilder) addWhereLessEqual(column string, value any) {
	qb.argCounter++
	qb.whereClauses = append(qb.whereClauses, fmt.Sprintf("%s <= $%d", column, qb.argCounter))
	qb.args = append(qb.args, value)
}
