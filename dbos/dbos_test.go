package dbos

import (
	"context"
	"fmt"
	"os"
	"testing"
)

var (
	w1 = WithWorkflow("userFunc1", userFunc1)
	s1 = WithStep("userStep1", userStep1)
)

func userFunc1(ctx context.Context, input string) (string, error) {
	fmt.Sprintf("I am a workflow: %s", input)
	res, err := s1(ctx, StepParams{}, input)
	return res, err
}

func userStep1(ctx context.Context, input string) (string, error) {
	return fmt.Sprintf("I am a step: %s", input), nil
}

func TestTransact(t *testing.T) {
	fmt.Println(registry)
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DBOS_DATABASE_URL not set, skipping integration test")
	}

	err := Launch()
	if err != nil {
		t.Fatalf("failed to create DBOS instance: %v", err)
	}
	defer Destroy()

	if dbos == nil {
		t.Fatal("expected DBOS instance but got nil")
	}

	wf1Handle, err := w1(context.Background(), WorkflowParams{WorkflowID: "wf1id"}, "no!")
	if err != nil {
		t.Fatalf("failed to run workflow: %v", err)
	}
	fmt.Println("Workflow handle:", wf1Handle)
	result, err := wf1Handle.GetResult()
	fmt.Printf("Workflow result: %s, error: %v\n", result, err)
}
