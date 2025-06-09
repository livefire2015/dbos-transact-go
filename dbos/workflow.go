package dbos

import (
	"context"
	"fmt"
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
	UpdatedAt          time.Time
	Timeout            time.Duration
	Deadline           time.Time // FIXME: maybe set this as an *int64 in milliseconds?
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
// TODO: for now this uses channels, but will be replaced with system DB lookups
func (h *workflowHandle[R]) GetResult() (R, error) {
	select {
	case result := <-h.resultChan:
		return result, nil
	case err := <-h.errorChan:
		var zero R
		return zero, err
	case <-h.ctx.Done():
		// This handles timeouts
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
	Deadline   time.Time
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

	// Compute the deadline
	var deadline time.Time
	if params.Timeout > 0 {
		deadline = time.Now().Add(params.Timeout)
	} else if !params.Deadline.IsZero() {
		deadline = params.Deadline
	}

	workflowStatus := WorkflowStatus{
		Status:    "PENDING",
		ID:        params.WorkflowID,
		CreatedAt: time.Now(),
		Deadline:  deadline,
	}

	// Init status // TODO: implement init status validation
	_, err := getExecutor().systemDB.InsertWorkflowStatus(ctx, workflowStatus)
	if err != nil {
		panic("failed to insert workflow status: " + err.Error())
	}

	// Channel to receive the result from the goroutine
	// The buffer size of 1 allows the goroutine to send the result without blocking
	// In addition it allows the channel to be garbage collected
	resultChan := make(chan R, 1)
	errorChan := make(chan error, 1)

	// Create the handle
	userFunctionContext := ctx
	var userFunctionCancel context.CancelFunc
	if params.Timeout > 0 {
		userFunctionContext, userFunctionCancel = context.WithTimeout(ctx, params.Timeout)
		defer userFunctionCancel() // Ensure the context is cancelled to free resources
	} else if !params.Deadline.IsZero() {
		userFunctionContext, userFunctionCancel = context.WithDeadline(ctx, params.Deadline)
		defer userFunctionCancel() // Ensure the context is cancelled to free resources
	}
	handle := &workflowHandle[R]{
		resultChan: resultChan,
		errorChan:  errorChan,
		ctx:        userFunctionContext,
	}

	// Run the function in a goroutine
	go func() {
		defer func() {
			// Avoid crashing the application if the workflow function panics
			if r := recover(); r != nil {
				errorChan <- fmt.Errorf("workflow function panicked: %v", r)
			}
		}()
		result, err := fn(ctx, input)
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- result
		}
	}()

	// Run the peer goroutine to handle cancellation and timeout
	go func() {
		select {
		case result := <-resultChan:
			// Record the result
			return
		case err := <-errorChan:
			// Record error
			return
		case <-userFunctionContext.Done():
			// the context was cancelled or timed out
			// Record timeout or cancellation
			return

		}
	}()

	return handle
}
