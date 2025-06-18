package dbos

/**
This suite tests high level DBOS features:
	- Calling workflows
		* Simple functions
		* Struct members
		* (all variations of "what can be workflow function")
		* With steps (and all their variations)
		* With child workflows (and all their variations)
	- Workflow handles
		* get result
		* get status
		* get wf ID
Specialized workflow features:
	- idempotency
	- timeout
	- deadlines

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
	simpleStp        = WithStep(simpleStep)
)

func simpleWorkflow(ctxt context.Context, input string) (string, error) {
	return input, nil
}

func simpleWorkflowError(ctx context.Context, input string) (int, error) {
	return 0, fmt.Errorf("failure")
}

func simpleWorkflowWithStep(ctx context.Context, input string) (string, error) {
	res, err := simpleStp(ctx, StepParams{}, input)
	return res, err
}

func simpleStep(ctx context.Context, input string) (string, error) {
	return "from step", nil
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
func TestSimpleWorflow(t *testing.T) {
	setupDBOS(t)

	handle, err := simpleWf(context.Background(), WorkflowParams{WorkflowID: uuid.NewString()}, "echo")
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}
	result, err := handle.GetResult()
	if err != nil {
		t.Fatal("expected no error")
	}
	if result != "echo" {
		t.Fatalf("unexpected return %s", result)
	}
}
func TestSimpleWorflowError(t *testing.T) {
	setupDBOS(t)

	handle, err := simpleWfError(context.Background(), WorkflowParams{WorkflowID: uuid.NewString()}, "echo")
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}
	_, err = handle.GetResult()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "failure" {
		t.Fatalf("unexpected error %s", err.Error())
	}
}

func TestSimpleWorflowWithStep(t *testing.T) {
	setupDBOS(t)

	handle, err := simpleWfWithStep(context.Background(), WorkflowParams{WorkflowID: uuid.NewString()}, "echo")
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}
	result, err := handle.GetResult()
	if err != nil {
		t.Fatal("expected no error")
	}
	if result != "from step" {
		t.Fatalf("unexpected return %s", result)
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
