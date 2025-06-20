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
	workflowIface WorkflowInterface = &workflowImplementation{
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
type WorkflowInterface interface {
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

/*
// Check list workflows
wfList, err := dbos.systemDB.ListWorkflows(context.Background(), ListWorkflowsDBInput{})
if err != nil {
	t.Fatalf("failed to list workflows: %v", err)
}
for _, wf := range wfList {
	fmt.Printf("Workflow ID: %s, Status: %s\n", wf.ID, wf.Status)
}
*/
