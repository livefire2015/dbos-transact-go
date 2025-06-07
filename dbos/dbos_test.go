package dbos

import (
	"context"
	"fmt"
	"os"
	"testing"
)

var (
	w1 = WithWorkflow("userFunc1", userFunc1)
)

func userFunc1(ctx context.Context, input string) (string, error) {
	if input == "no!" {
		return "yes!", nil
	}
	return input, nil
}

func TestTransact(t *testing.T) {
	fmt.Println(registry)
	// TEST DBOS OBJECT
	databaseURL := os.Getenv("DBOS_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("DBOS_DATABASE_URL not set, skipping integration test")
	}

	dbos, err := New()
	if err != nil {
		t.Fatalf("failed to create DBOS instance: %v", err)
	}
	defer dbos.Close()

	if dbos == nil {
		t.Fatal("expected DBOS instance but got nil")
	}

	// Test that we can access the system database
	systemDB := dbos.SystemDB()
	if systemDB == nil {
		t.Fatal("expected system database but got nil")
	}

	// Test closing
	if err := dbos.Close(); err != nil {
		t.Errorf("error closing DBOS: %v", err)
	}

	// TEST A WORKFLOW
	wf1Handle := w1(context.Background(), "no!")
	result, err := wf1Handle.GetResult()
	fmt.Printf("Workflow result: %s, error: %v\n", result, err)
}
