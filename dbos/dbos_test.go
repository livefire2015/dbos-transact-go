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

var (
	simpleWf         = WithWorkflow(simpleWorkflow)
	simpleWfError    = WithWorkflow(simpleWorkflowError)
	simpleWfWithStep = WithWorkflow(simpleWorkflowWithStep)
	// struct methods
	s              = workflowStruct{}
	simpleWfStruct = WithWorkflow(s.simpleWorkflow)
	simpleWfValue  = WithWorkflow(s.simpleWorkflowValue)
	// interface method workflow
	workflowIface WorkflowInterface = &workflowImplementation{}
	simpleWfIface                   = WithWorkflow(workflowIface.Execute)
	// Generic workflow
	wfInt = WithWorkflow(Identity[string]) // FIXME make this an int eventually
	// Closure with captured state
	prefix  = "hello-"
	wfClose = WithWorkflow(func(ctx context.Context, in string) (string, error) {
		return prefix + in, nil
	})
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

type workflowImplementation struct{}

func (w *workflowImplementation) Execute(ctx context.Context, input string) (string, error) {
	return input + "-interface", nil
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
			expectedResult: "echo-interface",
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
