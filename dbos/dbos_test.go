package dbos

/**
This suite tests high level DBOS features:
	[x] Wrapping various golang methods in DBODS workflows
	[] Workflow handles
	[] wf idempotency
	[] wf timeout
	[] wf deadlines
*/

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
)

// Global counter for idempotency testing
var idempotencyCounter int64

// Test struct types for encoding/decoding
type SimpleStruct struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

type StepInputStruct struct {
	Data SimpleStruct `json:"data"`
	ID   string       `json:"id"`
}

type StepOutputStruct struct {
	Input    StepInputStruct `json:"input"`
	StepInfo string          `json:"step_info"`
}

var (
	simpleWf                  = WithWorkflow(simpleWorkflow)
	simpleWfError             = WithWorkflow(simpleWorkflowError)
	simpleWfWithStep          = WithWorkflow(simpleWorkflowWithStep)
	simpleWfWithStepError     = WithWorkflow(simpleWorkflowWithStepError)
	simpleWfWithChildWorkflow = WithWorkflow(simpleWorkflowWithChildWorkflow)
	// struct methods
	s              = workflowStruct{}
	simpleWfStruct = WithWorkflow(s.simpleWorkflow)
	simpleWfValue  = WithWorkflow(s.simpleWorkflowValue)
	// interface method workflow
	workflowIface TestWorkflowInterface = &workflowImplementation{
		field: "example",
	}
	simpleWfIface = WithWorkflow(workflowIface.Execute)
	// Generic workflow
	wfInt = WithWorkflow(Identity[string]) // FIXME make this an int eventually
	// Closure with captured state
	prefix  = "hello-"
	wfClose = WithWorkflow(func(ctx context.Context, in string) (string, error) {
		return prefix + in, nil
	})
	// Workflow for idempotency testing
	idempotencyWf = WithWorkflow(idempotencyWorkflow)
	// Workflow for struct encoding testing
	structWfWithStep = WithWorkflow(structWorkflowWithStep)
)

func simpleWorkflow(ctxt context.Context, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(ctx context.Context, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(ctx context.Context, input string) (string, error) {
	return RunAsStep[string](ctx, StepParams{}, simpleStep, input)
}

func simpleStep(ctx context.Context, input string) (string, error) {
	return "from step", nil
}

func simpleStepError(ctx context.Context, input string) (string, error) {
	return "", fmt.Errorf("step failure")
}

func simpleWorkflowWithStepError(ctx context.Context, input string) (string, error) {
	return RunAsStep[string](ctx, StepParams{}, simpleStepError, input)
}

func simpleWorkflowWithChildWorkflow(ctx context.Context, input string) (string, error) {
	childHandle, err := simpleWfWithStep(ctx, WorkflowParams{}, input)
	if err != nil {
		return "", err
	}
	return childHandle.GetResult()
}

// idempotencyWorkflow increments a global counter and returns the input
func idempotencyWorkflow(ctx context.Context, input string) (string, error) {
	idempotencyCounter += 1
	return input, nil
}

// complexStructWorkflow processes a StepInputStruct using a step and returns the step result
func structWorkflowWithStep(ctx context.Context, input StepInputStruct) (StepOutputStruct, error) {
	return RunAsStep[StepInputStruct, StepOutputStruct](ctx, StepParams{}, simpleStructStep, input)
}

func simpleStructStep(ctx context.Context, input StepInputStruct) (StepOutputStruct, error) {
	return StepOutputStruct{
		Input:    input,
		StepInfo: "processed by simpleStructStep",
	}, nil
}

// Unified struct that demonstrates both pointer and value receiver methods
type workflowStruct struct{}

// Pointer receiver method
func (w *workflowStruct) simpleWorkflow(ctx context.Context, input string) (string, error) {
	return simpleWorkflow(ctx, input)
}

// Value receiver method on the same struct
func (w workflowStruct) simpleWorkflowValue(ctx context.Context, input string) (string, error) {
	return input + "-value", nil
}

// interface for workflow methods
type TestWorkflowInterface interface {
	Execute(ctx context.Context, input string) (string, error)
}

type workflowImplementation struct {
	field string
}

func (w *workflowImplementation) Execute(ctx context.Context, input string) (string, error) {
	return input + "-" + w.field + "-interface", nil
}

// Generic workflow function
func Identity[T any](ctx context.Context, in T) (T, error) {
	return in, nil
}

func setupDBOS(t *testing.T) {
	t.Helper()

	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DBOS_DATABASE_URL not set, skipping integration test")
	}

	err := Launch()
	if err != nil {
		t.Fatalf("failed to create DBOS instance: %v", err)
	}

	if dbos == nil {
		t.Fatal("expected DBOS instance but got nil")
	}

	// Register cleanup to run after test completes
	t.Cleanup(func() {
		Destroy()
	})
}
func TestWorkflowsWrapping(t *testing.T) {
	setupDBOS(t)

	// Eventually remove this, convenient for testing
	for k, v := range registry {
		fmt.Printf("Registered workflow: %s -> %T\n", k, v)
	}

	type testCase struct {
		name           string
		workflowFunc   func(context.Context, WorkflowParams, string) (any, error)
		input          string
		expectedResult any
		expectError    bool
		expectedError  string
	}

	tests := []testCase{
		{
			name: "SimpleWorkflow",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWf(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowError",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfError(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "failure",
		},
		{
			name: "SimpleWorkflowWithStep",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfWithStep(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowStruct",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfStruct(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo",
			expectError:    false,
		},
		{
			name: "ValueReceiverWorkflow",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfValue(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-value",
			expectError:    false,
		},
		{
			name: "interfaceMethodWorkflow",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfIface(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "echo-example-interface",
			expectError:    false,
		},
		{
			name: "GenericWorkflow",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				// For generic workflow, we need to convert string to int for testing
				handle, err := wfInt(ctx, params, "42") // FIXME for now this returns a string because sys db accepts this
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "42", // input not used in this case
			expectedResult: "42", // FIXME make this an int eventually
			expectError:    false,
		},
		{
			name: "ClosureWithCapturedState",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := wfClose(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "world",
			expectedResult: "hello-world",
			expectError:    false,
		},
		{
			name: "AnonymousClosure",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				// Create anonymous closure workflow directly in test
				anonymousWf := WithWorkflow(func(ctx context.Context, in string) (string, error) {
					return "anonymous-" + in, nil
				})
				handle, err := anonymousWf(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "test",
			expectedResult: "anonymous-test",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithChildWorkflow",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfWithChildWorkflow(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:          "echo",
			expectedResult: "from step",
			expectError:    false,
		},
		{
			name: "SimpleWorkflowWithStepError",
			workflowFunc: func(ctx context.Context, params WorkflowParams, input string) (any, error) {
				handle, err := simpleWfWithStepError(ctx, params, input)
				if err != nil {
					return nil, err
				}
				return handle.GetResult()
			},
			input:         "echo",
			expectError:   true,
			expectedError: "step failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := tc.workflowFunc(context.Background(), WorkflowParams{WorkflowID: uuid.NewString()}, tc.input)

			if tc.expectError {
				if err == nil {
					t.Fatal("expected error but got none")
				}
				if tc.expectedError != "" && err.Error() != tc.expectedError {
					t.Fatalf("expected error %q but got %q", tc.expectedError, err.Error())
				}
			} else {
				if err != nil {
					t.Fatalf("expected no error but got: %v", err)
				}
				if result != tc.expectedResult {
					t.Fatalf("expected result %v but got %v", tc.expectedResult, result)
				}
			}
		})
	}
}

func TestChildWorkflow(t *testing.T) {
	setupDBOS(t)

	t.Run("ChildWorkflowIDPattern", func(t *testing.T) {
		parentWorkflowID := uuid.NewString()

		// Create a parent workflow that calls a child workflow
		parentWf := WithWorkflow(func(ctx context.Context, input string) (string, error) {
			childHandle, err := simpleWfWithChildWorkflow(ctx, WorkflowParams{}, input)
			if err != nil {
				return "", err
			}

			// Verify child workflow ID follows the pattern: parentID-functionID
			childWorkflowID := childHandle.GetWorkflowID()
			expectedPrefix := parentWorkflowID + "-0"
			if childWorkflowID != expectedPrefix {
				return "", fmt.Errorf("expected child workflow ID to be %s, got %s", expectedPrefix, childWorkflowID)
			}

			return childHandle.GetResult()
		})

		// Execute the parent workflow
		parentHandle, err := parentWf(context.Background(), WorkflowParams{WorkflowID: parentWorkflowID}, "test-input")
		if err != nil {
			t.Fatalf("failed to execute parent workflow: %v", err)
		}

		// Verify the result
		result, err := parentHandle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}

		// The result should be from the child workflow's step
		expectedResult := "from step"
		if result != expectedResult {
			t.Fatalf("expected result %v but got %v", expectedResult, result)
		}
	})
}

func TestWorkflowIdempotency(t *testing.T) {
	setupDBOS(t)

	t.Run("WorkflowExecutedOnlyOnce", func(t *testing.T) {
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "idempotency-test"

		// Execute the same workflow twice with the same ID
		// First execution
		handle1, err := idempotencyWf(context.Background(), WorkflowParams{WorkflowID: workflowID}, input)
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}
		result1, err := handle1.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from first execution: %v", err)
		}

		// Second execution with the same workflow ID
		handle2, err := idempotencyWf(context.Background(), WorkflowParams{WorkflowID: workflowID}, input)
		if err != nil {
			t.Fatalf("failed to execute workflow second time: %v", err)
		}
		result2, err := handle2.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from second execution: %v", err)
		}

		// Verify both executions return the same result
		if result1 != result2 {
			t.Fatalf("expected same result from both executions, got %v and %v", result1, result2)
		}

		// Verify the counter was only incremented once (idempotency)
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to be 1 (workflow executed only once), but got %d", idempotencyCounter)
		}
	})
}

func TestWorkflowEncoding(t *testing.T) {
	setupDBOS(t)

	t.Run("BuiltInType", func(t *testing.T) {
		// Test a workflow that uses a built-in type (string)
		handle, err := simpleWf(context.Background(), WorkflowParams{}, "test")
		if err != nil {
			t.Fatalf("failed to execute workflow: %v", err)
		}

		// Block until the workflow completes
		_, err = handle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}

		retrieveHandler, err := RetrieveWorkflow[string](handle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to retrieve workflow: %v", err)
		}
		retrievedResult, err := retrieveHandler.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}
		if retrievedResult != "test" {
			t.Fatalf("expected retrieved result to be 'test', got %v", retrievedResult)
		}
	})

	t.Run("StructType", func(t *testing.T) {
		// Test a workflow that calls a step with struct types to verify serialization/deserialization
		input := SimpleStruct{Name: "test", Value: 123}
		stepInput := StepInputStruct{
			Data: input,
			ID:   "step-test",
		}

		stepHandle, err := structWfWithStep(context.Background(), WorkflowParams{}, stepInput)
		if err != nil {
			t.Fatalf("failed to execute step workflow: %v", err)
		}

		// Block until the workflow completes
		_, err = stepHandle.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}

		// Test output deserialization from the workflow_status table
		stepRetrieveHandler, err := RetrieveWorkflow[StepOutputStruct](stepHandle.GetWorkflowID())
		if err != nil {
			t.Fatalf("failed to retrieve step workflow: %v", err)
		}
		stepRetrievedResult, err := stepRetrieveHandler.GetResult()
		if err != nil {
			t.Fatalf("expected no error but got: %v", err)
		}

		if stepRetrievedResult.Input.ID != stepInput.ID {
			t.Fatalf("expected step input ID to be %v, got %v", stepInput.ID, stepRetrievedResult.Input.ID)
		}
		if stepRetrievedResult.Input.Data.Name != input.Name {
			t.Fatalf("expected step input data name to be %v, got %v", input.Name, stepRetrievedResult.Input.Data.Name)
		}
		if stepRetrievedResult.Input.Data.Value != input.Value {
			t.Fatalf("expected step input data value to be %v, got %v", input.Value, stepRetrievedResult.Input.Data.Value)
		}
		if stepRetrievedResult.StepInfo != "processed by simpleStructStep" {
			t.Fatalf("expected step info to be 'processed by simpleStructStep', got %v", stepRetrievedResult.StepInfo)
		}

		// Test input/output deserialization from the workflow_status table
		workflows, err := getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{stepHandle.GetWorkflowID()},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}
		workflow := workflows[0]
		if workflow.Input == nil {
			t.Fatal("expected workflow input to be non-nil")
		}
		workflowInput, ok := workflow.Input.(StepInputStruct)
		if !ok {
			t.Fatalf("expected workflow input to be of type StepInputStruct, got %T", workflow.Input)
		}
		if workflowInput.ID != stepInput.ID {
			t.Fatalf("expected workflow input ID to be %v, got %v", stepInput.ID, workflowInput.ID)
		}
		if workflowInput.Data.Name != input.Name {
			t.Fatalf("expected workflow input data name to be %v, got %v", input.Name, workflowInput.Data.Name)
		}
		if workflowInput.Data.Value != input.Value {
			t.Fatalf("expected workflow input data value to be %v, got %v", input.Value, workflowInput.Data.Value)
		}

		workflowOutput, ok := workflow.Output.(StepOutputStruct)
		if !ok {
			t.Fatalf("expected workflow output to be of type StepOutputStruct, got %T", workflow.Output)
		}
		if workflowOutput.Input.ID != stepInput.ID {
			t.Fatalf("expected workflow output input ID to be %v, got %v", stepInput.ID, workflowOutput.Input.ID)
		}
		if workflowOutput.Input.Data.Name != input.Name {
			t.Fatalf("expected workflow output input data name to be %v, got %v", input.Name, workflowOutput.Input.Data.Name)
		}
		if workflowOutput.Input.Data.Value != input.Value {
			t.Fatalf("expected workflow output input data value to be %v, got %v", input.Value, workflowOutput.Input.Data.Value)
		}
		if workflowOutput.StepInfo != "processed by simpleStructStep" {
			t.Fatalf("expected workflow output step info to be 'processed by simpleStructStep', got %v", workflowOutput.StepInfo)
		}

		// TODO: test output deserialization from the operation_results table
	})
}
func TestWorkflowRecovery(t *testing.T) {
	setupDBOS(t)

	// Recovering a pending workflow restart where it left off
	// Test recovery of specified executor only
	// Test recovery of specific version only
	// Test recovery reach max recovery attempts
	t.Run("RecoveryResumeWhereItLeftOff", func(t *testing.T) {

		// TODO have this use a step

		// Reset the global counter
		idempotencyCounter = 0

		workflowID := uuid.NewString()
		input := "recovery-test"

		// First execution - run the workflow once
		handle1, err := idempotencyWf(context.Background(), WorkflowParams{WorkflowID: workflowID}, input)
		if err != nil {
			t.Fatalf("failed to execute workflow first time: %v", err)
		}

		// Wait for completion and get result
		result1, err := handle1.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from first execution: %v", err)
		}

		// expectedResult := fmt.Sprintf("step-result-%s", input)
		expectedResult := input
		if result1 != expectedResult {
			t.Fatalf("expected result %v but got %v", expectedResult, result1)
		}

		// Verify the counter was incremented once
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to be 1 after first execution, but got %d", idempotencyCounter)
		}

		// Reset workflow status back to PENDING to simulate recovery scenario
		err = getExecutor().systemDB.UpdateWorkflowOutcome(context.Background(), UpdateWorkflowOutcomeDBInput{
			workflowID: workflowID,
			status:     WorkflowStatusPending,
		})
		if err != nil {
			t.Fatalf("failed to reset workflow status to PENDING: %v", err)
		}

		// Run recovery for pending workflows with "local" executor
		recoveredHandles, err := recoverPendingWorkflows(context.Background(), []string{"local"})
		if err != nil {
			t.Fatalf("failed to recover pending workflows: %v", err)
		}

		// Check that we have a single handle in the return list
		if len(recoveredHandles) != 1 {
			t.Fatalf("expected 1 recovered handle, got %d", len(recoveredHandles))
		}

		// Check that the workflow ID from the handle is the same as the first handle
		recoveredHandle := recoveredHandles[0]
		if recoveredHandle.GetWorkflowID() != handle1.GetWorkflowID() {
			t.Fatalf("expected recovered workflow ID %s, got %s", handle1.GetWorkflowID(), recoveredHandle.GetWorkflowID())
		}

		// Wait for recovery to complete
		result2, err := recoveredHandle.GetResult()
		if err != nil {
			t.Fatalf("failed to get result from recovered execution: %v", err)
		}

		// Ensure the result is the same
		if result2 != result1 {
			t.Fatalf("expected recovered result %v to match original result %v", result2, result1)
		}

		// Ensure that the idempotency counter wasn't incremented again
		if idempotencyCounter != 1 {
			t.Fatalf("expected counter to remain 1 after recovery (idempotent), but got %d", idempotencyCounter)
		}

		// Using ListWorkflows, retrieve the status of the workflow
		workflows, err := getExecutor().systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{
			WorkflowIDs: []string{workflowID},
		})
		if err != nil {
			t.Fatalf("failed to list workflows: %v", err)
		}

		if len(workflows) != 1 {
			t.Fatalf("expected 1 workflow, got %d", len(workflows))
		}

		workflow := workflows[0]

		// Ensure that its status is SUCCESS
		if workflow.Status != WorkflowStatusSuccess {
			t.Fatalf("expected workflow status to be SUCCESS, got %s", workflow.Status)
		}

		// Ensure its number of attempts is 2
		if workflow.Attempts != 2 {
			t.Fatalf("expected workflow attempts to be 2, got %d", workflow.Attempts)
		}

		// Ensure its output is the same as the output of the first invocation
		workflowOutput, ok := workflow.Output.(string)
		if !ok {
			t.Fatalf("expected workflow output to be string, got %T", workflow.Output)
		}
		if workflowOutput != expectedResult {
			t.Fatalf("expected workflow output %v, got %v", expectedResult, workflowOutput)
		}
	})
}

/*
Add tests for:
- Workflow attempt counter


*/
