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

// WorkflowState holds the runtime state for a workflow execution
type WorkflowState struct {
	WorkflowID  string
	stepCounter int
}

// NextStepID returns the next step ID and increments the counter
func (ws *WorkflowState) NextStepID() int {
	ws.stepCounter++
	return ws.stepCounter
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

/**********************************/
/******* WORKFLOW FUNCTIONS *******/
/**********************************/
type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)

type WorkflowParams struct {
	WorkflowID string
	Timeout    time.Duration
	Deadline   time.Time
}

func WithWorkflow[P any, R any](fn WorkflowFunc[P, R]) func(ctx context.Context, params WorkflowParams, input P) (WorkflowHandle[R], error) {
	// Also we should register the wrapped function
	registerWorkflow(fn)
	return func(ctx context.Context, params WorkflowParams, input P) (WorkflowHandle[R], error) {
		return runAsWorkflow(ctx, params, fn, input)
	}
}

// TODO also return errors
func runAsWorkflow[P any, R any](ctx context.Context, params WorkflowParams, fn WorkflowFunc[P, R], input P) (WorkflowHandle[R], error) {
	// First, create a context for the workflow
	dbosWorkflowContext := context.Background()

	// TODO Check if cancelled
	// TODO Check if a result/error is already available

	// Compute the context deadline if any
	var deadline time.Time
	if params.Timeout > 0 {
		deadline = time.Now().Add(params.Timeout)
	} else if !params.Deadline.IsZero() {
		deadline = params.Deadline
	}

	var workflowCancelFunction context.CancelFunc
	if params.Timeout > 0 {
		dbosWorkflowContext, workflowCancelFunction = context.WithTimeout(dbosWorkflowContext, params.Timeout)
		defer workflowCancelFunction() // Ensure the context is cancelled to free resources
	} else if !params.Deadline.IsZero() {
		dbosWorkflowContext, workflowCancelFunction = context.WithDeadline(dbosWorkflowContext, params.Deadline)
		defer workflowCancelFunction() // Ensure the context is cancelled to free resources
	}

	// Generate an ID for the workflow if not provided
	if params.WorkflowID == "" {
		params.WorkflowID = uuid.New().String()
	}

	workflowStatus := WorkflowStatus{
		Status:    "PENDING",
		ID:        params.WorkflowID,
		CreatedAt: time.Now(),
		Deadline:  deadline,
	}

	// Init status // TODO: implement init status validation
	_, err := getExecutor().systemDB.InsertWorkflowStatus(dbosWorkflowContext, workflowStatus)
	if err != nil {
		return nil, fmt.Errorf("inserting workflow status: %w", err)
	}

	// Channel to receive the result from the goroutine
	// The buffer size of 1 allows the goroutine to send the result without blocking
	// In addition it allows the channel to be garbage collected
	resultChan := make(chan R, 1)
	errorChan := make(chan error, 1)

	// Create the handle
	handle := &workflowHandle[R]{
		resultChan: resultChan,
		errorChan:  errorChan,
		ctx:        dbosWorkflowContext,
	}

	// Create workflow state to track step execution
	workflowState := &WorkflowState{
		WorkflowID:  workflowStatus.ID,
		stepCounter: 0,
	}

	// Run the function in a goroutine
	augmentUserContext := context.WithValue(ctx, "workflowState", workflowState)
	go func() {
		result, err := fn(augmentUserContext, input)
		if err != nil {
			fmt.Println("workflow function returned an error:", err)
			recordErr := getExecutor().systemDB.RecordWorkflowError(dbosWorkflowContext, workflowErrorDBInput{workflowID: workflowStatus.ID, err: err})
			if recordErr != nil {
				// TODO: make sure to return both errors
				fmt.Println("recording workflow error:", recordErr)
				errorChan <- recordErr
				return
			}
			errorChan <- err
		} else {
			recordErr := getExecutor().systemDB.RecordWorkflowOutput(dbosWorkflowContext, workflowOutputDBInput{workflowID: workflowStatus.ID, output: result})
			if recordErr != nil {
				fmt.Println("recording workflow output:", recordErr)
				// We cannot return the user code result because we failed to record the output
				errorChan <- recordErr
				return
			}
			resultChan <- result
		}
	}()

	// Run the peer goroutine to handle cancellation and timeout
	if dbosWorkflowContext.Done() != nil {
		fmt.Println("starting goroutine to handle workflow context cancellation or timeout")
		go func() {
			select {
			case <-dbosWorkflowContext.Done():
				// The context was cancelled or timed out: record timeout or cancellation
				timeoutErr := dbosWorkflowContext.Err()
				err := getExecutor().systemDB.RecordWorkflowError(dbosWorkflowContext, workflowErrorDBInput{
					workflowID: workflowStatus.ID,
					err:        timeoutErr,
				})
				if err != nil {
					fmt.Println("failed to record workflow error:", err)
				}
				return
			}
		}()
	}

	fmt.Println("Returning workflow handle for workflow ID:", params.WorkflowID)
	return handle, nil
}

/******************************/
/******* STEP FUNCTIONS *******/
/******************************/

type StepFunc[P any, R any] func(ctx context.Context, input P) (R, error)

type StepParams struct {
	MaxAttempts int
	BackoffRate int
}

func WithStep[P any, R any](fn StepFunc[P, R]) func(ctx context.Context, params StepParams, input P) (R, error) {
	// TODO : name can be found using reflection. Must be FQDN.
	registerWorkflow(fn)
	return func(ctx context.Context, params StepParams, input P) (R, error) {
		return runAsStep(ctx, params, fn, input)
	}
}

func runAsStep[P any, R any](ctx context.Context, params StepParams, fn StepFunc[P, R], input P) (R, error) {
	// Get workflow state from context
	workflowState, ok := ctx.Value("workflowState").(*WorkflowState)
	if !ok || workflowState == nil {
		return *new(R), fmt.Errorf("context does not contain valid workflow state, cannot run step")
	}

	// Check if cancelled
	// Check if a result/error is already available

	// Get next step ID
	stepID := workflowState.NextStepID()

	stepOutput, stepError := fn(ctx, input)
	if stepError != nil {
		fmt.Println("step function returned an error:", stepError)
		err := getExecutor().systemDB.RecordOperationResult(ctx, recordOperationResultDBInput{
			workflowID:  workflowState.WorkflowID,
			operationID: stepID,
			err:         stepError,
		})
		if err != nil {
			fmt.Println("failed to record step error:", err)
			return *new(R), fmt.Errorf("failed to record step error: %w", err)
		}
	} else {
		err := getExecutor().systemDB.RecordOperationResult(ctx, recordOperationResultDBInput{
			workflowID:  workflowState.WorkflowID,
			operationID: stepID,
			output:      stepOutput,
		})
		if err != nil {
			fmt.Println("failed to record step output:", err)
			return *new(R), fmt.Errorf("failed to record step output: %w", err)
		}
	}
	return stepOutput, stepError
}
