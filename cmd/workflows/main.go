package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/dbos-inc/dbos-transact-go/dbos"
)

func workflow(dbosCtx dbos.DBOSContext, name string) (string, error) {
	fmt.Printf("Starting workflow for %s\n", name)
	
	// Step 1: Process initialization
	result1, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepOne(ctx, name)
	})
	if err != nil {
		return "", fmt.Errorf("step one failed: %w", err)
	}
	
	// Step 2: Data processing
	result2, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepTwo(ctx, result1)
	})
	if err != nil {
		return "", fmt.Errorf("step two failed: %w", err)
	}
	
	// Step 3: Finalization
	finalResult, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return stepThree(ctx, result2)
	})
	if err != nil {
		return "", fmt.Errorf("step three failed: %w", err)
	}
	
	return finalResult, nil
}

func stepOne(ctx context.Context, name string) (string, error) {
	fmt.Printf("Step 1: Initializing for %s\n", name)
	// Simulate some work
	return fmt.Sprintf("Initialized data for %s", name), nil
}

func stepTwo(ctx context.Context, input string) (string, error) {
	fmt.Printf("Step 2: Processing - %s\n", input)
	// Simulate processing
	return fmt.Sprintf("Processed: %s", input), nil
}

func stepThree(ctx context.Context, input string) (string, error) {
	fmt.Printf("Step 3: Finalizing - %s\n", input)
	// Simulate finalization
	return fmt.Sprintf("Completed workflow! %s", input), nil
}

func main() {
	// Get database URL from environment or use default
	dbURL := os.Getenv("DBOS_SYSTEM_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:pwd@localhost:5432/dbos?sslmode=disable"
		fmt.Printf("Using default database URL: %s\n", dbURL)
	}

	// Initialize DBOS context
	ctx, err := dbos.NewDBOSContext(dbos.Config{
		DatabaseURL: dbURL,
		AppName:     "workflows-example",
	})
	if err != nil {
		log.Fatalf("Failed to create DBOS context: %v", err)
	}

	// Register the workflow
	dbos.RegisterWorkflow(ctx, workflow)

	// Launch DBOS runtime
	err = ctx.Launch()
	if err != nil {
		log.Fatalf("Failed to launch DBOS: %v", err)
	}
	defer ctx.Cancel()

	fmt.Println("DBOS Workflows Example Started")
	fmt.Println("=====================================")

	// Run multiple workflows
	workflows := []string{"Alice", "Bob", "Charlie"}
	handles := make([]dbos.WorkflowHandle[string], len(workflows))

	for i, name := range workflows {
		fmt.Printf("Starting workflow for %s...\n", name)
		handle, err := dbos.RunAsWorkflow(ctx, workflow, name)
		if err != nil {
			log.Printf("Failed to start workflow for %s: %v", name, err)
			continue
		}
		handles[i] = handle
	}

	// Get results
	fmt.Println("\nWaiting for workflows to complete...")
	for i, handle := range handles {
		if handle == nil {
			continue
		}
		result, err := handle.GetResult()
		if err != nil {
			log.Printf("Workflow %d failed: %v", i, err)
		} else {
			fmt.Printf("Workflow %d result: %s\n", i, result)
		}
	}

	fmt.Println("\n✅ Durable Workflows Example Complete!")
	fmt.Println("Try killing this process mid-execution and restarting - workflows will resume from the last completed step!")
}