package dbos

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type WorkflowStatus struct {
	Name               string
	Status             string // TODO make an enum type
	ID                 string
	ExecutorID         string
	ApplicationVersion *string
	ApplicationID      string
	CreatedAt          time.Time
	UpdatedAt          *time.Time
	Timeout            time.Duration
	Deadline           *time.Time // FIXME: maybe set this as an *int64 in milliseconds?
	Input              any
	Attempts           int
	// Add remaining fields
}

/********************************/
/******* WORKFLOW HANDLE ********/
/********************************/
type WorkflowHandle[R any] interface {
	GetResult() (R, error)
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	resultChan chan R
	errorChan  chan error
	ctx        context.Context
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult() (R, error) {
	select {
	case result := <-h.resultChan:
		return result, nil
	case err := <-h.errorChan:
		var zero R
		return zero, err
	case <-h.ctx.Done():
		var zero R
		return zero, h.ctx.Err()
	}
}

/********************************/
/******* WORKFLOW FUNCTION *******/
/********************************/
type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)

type WorkflowParams struct {
	WorkflowID string
	Timeout    time.Duration
}

func WithWorkflow[P any, R any](name string, fn WorkflowFunc[P, R]) func(ctx context.Context, params WorkflowParams, input P) WorkflowHandle[R] {
	// TODO: name can be found using reflection. Must be FQDN.
	registerWorkflow(name, fn)
	return func(ctx context.Context, params WorkflowParams, input P) WorkflowHandle[R] {
		return runAsWorkflow(ctx, params, fn, input)
	}
}

func runAsWorkflow[P any, R any](ctx context.Context, params WorkflowParams, fn WorkflowFunc[P, R], input P) WorkflowHandle[R] {
	// Generate an ID for the workflow if not provided
	if params.WorkflowID == "" {
		params.WorkflowID = uuid.New().String()
	}

	workflowStatus := WorkflowStatus{
		Status:    "PENDING",
		ID:        params.WorkflowID,
		CreatedAt: time.Now(),
	}

	// Init status // TODO: implement init status validation
	_, err := getExecutor().systemDB.InsertWorkflowStatus(ctx, workflowStatus)
	if err != nil {
		panic("failed to insert workflow status: " + err.Error())
	}

	// Channel to receive the result from the goroutine
	resultChan := make(chan R, 1)
	errorChan := make(chan error, 1)

	// Create the handle
	handle := &workflowHandle[R]{
		resultChan: resultChan,
		errorChan:  errorChan,
		ctx:        ctx,
	}

	// Run the function in a goroutine
	go func() {
		result, err := fn(ctx, input)
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- result
		}
	}()

	return handle
}
