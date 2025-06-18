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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

/*******************************/
/******* INTERFACE ********/
/*******************************/

type SystemDatabase interface {
	Destroy()
	InsertWorkflowStatus(ctx context.Context, initStatus WorkflowStatus) (*InsertWorkflowResult, error)
	RecordWorkflowOutput(ctx context.Context, input workflowOutputDBInput) error
	RecordWorkflowError(ctx context.Context, input workflowErrorDBInput) error
	RecordOperationResult(ctx context.Context, input recordOperationResultDBInput) error
	ListWorkflows(ctx context.Context, input ListWorkflowsDBInput) ([]WorkflowStatus, error)
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

func (s *systemDatabase) InsertWorkflowStatus(ctx context.Context, initStatus WorkflowStatus) (*InsertWorkflowResult, error) {
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

	// TODO eventually handle any input type
	// Convert input to string if it's not nil
	var inputStr string
	if initStatus.Input != nil {
		inputStr = fmt.Sprintf("%v", initStatus.Input)
	}

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
	err := s.pool.QueryRow(ctx, query,
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
		inputStr,
		initStatus.DeduplicationID,
		initStatus.Priority,
	).Scan(
		&result.Attempts,
		&result.Status,
		&result.Name,
		&result.QueueName,
		&result.WorkflowDeadlineEpochMs,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to insert workflow status: %w", err)
	}

	return &result, nil
}

// FIXME: merge in a single function that knows whether to set error or output
type workflowOutputDBInput struct {
	workflowID string
	output     any // XXX This will be updated to reflect that other types than string can be recorded
}

func (s *systemDatabase) RecordWorkflowOutput(ctx context.Context, input workflowOutputDBInput) error {
	updateInput := UpdateWorkflowStatusDBInput{
		WorkflowID: input.workflowID,
		Status:     "SUCCESS",
		Options: UpdateWorkflowStatusOptions{
			Update: &UpdateWorkflowStatusUpdate{
				Output: input.output,
			},
		},
	}
	return s.UpdateWorkflowStatus(ctx, updateInput)
}

type workflowErrorDBInput struct {
	workflowID string
	err        error
}

func (s *systemDatabase) RecordWorkflowError(ctx context.Context, input workflowErrorDBInput) error {
	updateInput := UpdateWorkflowStatusDBInput{
		WorkflowID: input.workflowID,
		Status:     "ERROR",
		Options: UpdateWorkflowStatusOptions{
			Update: &UpdateWorkflowStatusUpdate{
				Error: input.err,
			},
		},
	}
	return s.UpdateWorkflowStatus(ctx, updateInput)
}

type recordOperationResultDBInput struct {
	workflowID    string
	operationID   int
	operationName string
	output        any
	err           error
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

	commandTag, err := s.pool.Exec(ctx, query,
		input.workflowID,
		input.operationID,
		input.output,
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
		var inputStr *string

		err := rows.Scan(
			&wf.ID, &wf.Status, &wf.Name, &wf.AuthenticatedUser, &wf.AssumedRole,
			&wf.AuthenticatedRoles, &wf.Output, &wf.Error, &wf.ExecutorID, &createdAtMs,
			&updatedAtMs, &wf.ApplicationVersion, &wf.ApplicationID,
			&wf.Attempts, &wf.QueueName, &timeoutMs,
			&deadlineMs, &startedAtMs, &wf.DeduplicationID,
			&inputStr, &wf.Priority,
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

		// TODO: Convert inputStr back to proper type - for now just store as string
		if inputStr != nil {
			wf.Input = *inputStr
		}

		workflows = append(workflows, wf)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over workflow rows: %w", err)
	}

	return workflows, nil
}

type UpdateWorkflowStatusDBInput struct {
	WorkflowID string
	Status     string
	Options    UpdateWorkflowStatusOptions
}

type UpdateWorkflowStatusOptions struct {
	Update         *UpdateWorkflowStatusUpdate
	Where          *UpdateWorkflowStatusWhere
	ThrowOnFailure *bool
	Tx             pgx.Tx
}

type UpdateWorkflowStatusUpdate struct {
	Output                any
	Error                 error
	ResetRecoveryAttempts bool
	QueueName             *string
	ResetDeadline         bool
	ResetDeduplicationID  bool
	ResetStartedAtEpochMs bool
}

type UpdateWorkflowStatusWhere struct {
	Status *string
}

func (s *systemDatabase) UpdateWorkflowStatus(ctx context.Context, input UpdateWorkflowStatusDBInput) error {
	qb := newQueryBuilder()

	// Always set these
	qb.addSet("status", input.Status)
	qb.addSet("updated_at", time.Now().UnixMilli())
	qb.addWhere("workflow_uuid", input.WorkflowID)

	// Handle update options
	if update := input.Options.Update; update != nil {
		if update.Output != nil {
			qb.addSet("output", update.Output.(string))
		}
		if update.Error != nil {
			qb.addSet("error", update.Error.Error())
		}
		if update.ResetRecoveryAttempts {
			qb.addSetRaw("recovery_attempts = 0")
		}
		if update.ResetDeadline {
			qb.addSetRaw("workflow_deadline_epoch_ms = NULL")
		}
		if update.QueueName != nil {
			qb.addSet("queue_name", *update.QueueName)
		}
		if update.ResetDeduplicationID {
			qb.addSetRaw("deduplication_id = NULL")
		}
		if update.ResetStartedAtEpochMs {
			qb.addSetRaw("started_at_epoch_ms = NULL")
		}
	}

	// Handle where options
	if where := input.Options.Where; where != nil && where.Status != nil {
		qb.addWhere("status", *where.Status)
	}

	// Build and execute query
	query := fmt.Sprintf("UPDATE dbos.workflow_status SET %s WHERE %s",
		strings.Join(qb.setClauses, ", "),
		strings.Join(qb.whereClauses, " AND "))

	// Execute the query using transaction if provided, otherwise use connection pool
	var commandTag pgconn.CommandTag
	var err error

	if input.Options.Tx != nil {
		commandTag, err = input.Options.Tx.Exec(ctx, query, qb.args...)
	} else {
		commandTag, err = s.pool.Exec(ctx, query, qb.args...)
	}

	if err != nil {
		return fmt.Errorf("failed to update workflow status: %w", err)
	}

	// Check if update was successful
	throwOnFailure := input.Options.ThrowOnFailure == nil || *input.Options.ThrowOnFailure
	if throwOnFailure && commandTag.RowsAffected() != 1 {
		return fmt.Errorf("attempt to record transition of nonexistent workflow %s", input.WorkflowID)
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

	updateInput := UpdateWorkflowStatusDBInput{
		WorkflowID: workflowID,
		Status:     "CANCELLED",
		Options: UpdateWorkflowStatusOptions{
			Update: &UpdateWorkflowStatusUpdate{
				QueueName:             nil,
				ResetDeduplicationID:  true,
				ResetStartedAtEpochMs: true,
			},
			Tx: tx,
		},
	}

	if err := s.UpdateWorkflowStatus(ctx, updateInput); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
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
