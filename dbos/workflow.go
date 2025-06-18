package dbos

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
)

/*******************************/
/******* WORKFLOW STATUS *******/
/*******************************/

type WorkflowStatusType string

const (
	WorkflowStatusPending         WorkflowStatusType = "PENDING"
	WorkflowStatusEnqueued        WorkflowStatusType = "ENQUEUED"
	WorkflowStatusSuccess         WorkflowStatusType = "SUCCESS"
	WorkflowStatusError           WorkflowStatusType = "ERROR"
	WorkflowStatusCancelled       WorkflowStatusType = "CANCELLED"
	WorkflowStatusRetriesExceeded WorkflowStatusType = "RETRIES_EXCEEDED"
)

type WorkflowStatus struct {
	ID                 string             `json:"workflow_uuid"`
	Status             WorkflowStatusType `json:"status"`
	Name               string             `json:"name"`
	AuthenticatedUser  *string            `json:"authenticated_user"`
	AssumedRole        *string            `json:"assumed_role"`
	AuthenticatedRoles *string            `json:"authenticated_roles"`
	Output             any                `json:"output"`
	Error              error              `json:"error"`
	ExecutorID         *string            `json:"executor_id"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
	ApplicationVersion *string            `json:"application_version"`
	ApplicationID      *string            `json:"application_id"`
	Attempts           int                `json:"attempts"`
	QueueName          *string            `json:"queue_name"`
	Timeout            time.Duration      `json:"timeout"`
	Deadline           time.Time          `json:"deadline"`
	StartedAt          time.Time          `json:"started_at"`
	DeduplicationID    *string            `json:"deduplication_id"`
	Input              any                `json:"input"`
	Priority           int                `json:"priority"`
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
	workflowID string
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

func runAsWorkflow[P any, R any](ctx context.Context, params WorkflowParams, fn WorkflowFunc[P, R], input P) (WorkflowHandle[R], error) {
	// First, create a context for the workflow
	dbosWorkflowContext := context.Background()

	// TODO Check if cancelled
	// TODO Check if a result/error is already available

	// Generate an ID for the workflow if not provided
	if params.WorkflowID == "" {
		params.WorkflowID = uuid.New().String()
	}

	workflowStatus := WorkflowStatus{
		Status:        WorkflowStatusPending,
		ID:            params.WorkflowID,
		CreatedAt:     time.Now(),
		Deadline:      params.Deadline,
		Timeout:       params.Timeout,
		Input:         input,
		ApplicationID: nil, // TODO: set application ID if available
		QueueName:     nil, // TODO: set queue name if available
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
		workflowID: workflowStatus.ID,
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
			recordErr := getExecutor().systemDB.UpdateWorkflowOutcome(dbosWorkflowContext, UpdateWorkflowOutcomeDBInput{workflowID: workflowStatus.ID, status: WorkflowStatusError, err: err})
			if recordErr != nil {
				// TODO: make sure to return both errors
				errorChan <- recordErr
				return
			}
			errorChan <- err
		} else {
			recordErr := getExecutor().systemDB.UpdateWorkflowOutcome(dbosWorkflowContext, UpdateWorkflowOutcomeDBInput{workflowID: workflowStatus.ID, status: WorkflowStatusSuccess, output: result})
			if recordErr != nil {
				// We cannot return the user code result because we failed to record the output
				errorChan <- recordErr
				return
			}
			resultChan <- result
		}
	}()

	// Run the peer goroutine to handle cancellation and timeout
	/*
		if dbosWorkflowContext.Done() != nil {
			fmt.Println("starting goroutine to handle workflow context cancellation or timeout")
			go func() {
				select {
				case <-dbosWorkflowContext.Done():
					// The context was cancelled or timed out: record timeout or cancellation
					// CANCEL WORKFLOW HERE
					return
				}
			}()
		}
	*/

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

func RunAsStep[R any](ctx context.Context, params StepParams, fn any, input ...any) (R, error) {
	// Get workflow state from context
	workflowState, ok := ctx.Value("workflowState").(*WorkflowState)
	if !ok || workflowState == nil {
		return *new(R), fmt.Errorf("context does not contain valid workflow state, cannot run step")
	}

	// Check if cancelled
	// Check if a result/error is already available

	// Get next step ID
	stepID := workflowState.NextStepID()

	// Use reflection to call the function with the proper arguments
	results, err := callFunction(fn, append([]any{ctx}, input...)...)
	if err != nil {
		fmt.Println("step function call failed:", err)
		recordErr := getExecutor().systemDB.RecordOperationResult(ctx, recordOperationResultDBInput{
			workflowID:  workflowState.WorkflowID,
			operationID: stepID,
			err:         err,
		})
		if recordErr != nil {
			fmt.Println("failed to record step error:", recordErr)
			return *new(R), fmt.Errorf("failed to record step error: %w", recordErr)
		}
		return *new(R), err
	}

	// Extract the result and error from the function call
	var stepOutput R
	var stepError error

	if len(results) >= 1 {
		if result, ok := results[0].(R); ok {
			stepOutput = result
		}
	}
	if len(results) >= 2 {
		if err, ok := results[1].(error); ok {
			stepError = err
		}
	}

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

// callFunction uses reflection to call a function with the given arguments
func callFunction(fn any, args ...any) ([]any, error) {
	fnValue := reflect.ValueOf(fn)
	fnType := fnValue.Type()

	if fnValue.Kind() != reflect.Func {
		return nil, fmt.Errorf("provided value is not a function")
	}

	// Check if the number of arguments matches
	if fnType.NumIn() != len(args) {
		return nil, fmt.Errorf("function expects %d arguments, got %d", fnType.NumIn(), len(args))
	}

	// Convert arguments to reflect.Value and validate types
	argValues := make([]reflect.Value, len(args))
	for i, arg := range args {
		argValue := reflect.ValueOf(arg)
		expectedType := fnType.In(i)

		// Check if the argument type is assignable to the expected parameter type
		if !argValue.Type().AssignableTo(expectedType) {
			return nil, fmt.Errorf("argument %d: expected type %s, got %s",
				i, expectedType.String(), argValue.Type().String())
		}

		argValues[i] = argValue
	}

	// Call the function
	resultValues := fnValue.Call(argValues)

	// Convert results back to interface{}
	results := make([]any, len(resultValues))
	for i, result := range resultValues {
		results[i] = result.Interface()
	}

	return results, nil
}
