package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-go/dbos"
)

// Task workflow that will be queued
func processTask(ctx dbos.DBOSContext, taskData TaskData) (string, error) {
	fmt.Printf("[Task %d] Starting processing: %s\n", taskData.ID, taskData.Name)
	
	// Simulate some processing work
	ctx.Sleep(2 * time.Second)
	
	fmt.Printf("[Task %d] Completed: %s\n", taskData.ID, taskData.Name)
	return fmt.Sprintf("Task %d (%s) processed successfully at %s", 
		taskData.ID, taskData.Name, time.Now().Format("15:04:05")), nil
}

// TaskData represents the data passed to each queued task
type TaskData struct {
	ID   int
	Name string
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
		AppName:     "queues-example",
	})
	if err != nil {
		log.Fatalf("Failed to create DBOS context: %v", err)
	}

	// Register the task workflow
	dbos.RegisterWorkflow(ctx, processTask)

	// Create queues with different configurations
	
	// Standard queue with worker concurrency limit
	standardQueue := dbos.NewWorkflowQueue(ctx, "standard-queue",
		dbos.WithWorkerConcurrency(2), // Process max 2 tasks concurrently per worker
	)

	// Priority queue for high-priority tasks
	priorityQueue := dbos.NewWorkflowQueue(ctx, "priority-queue",
		dbos.WithPriorityEnabled(true),
		dbos.WithWorkerConcurrency(3),
	)

	// Rate-limited queue for external API calls
	rateLimitedQueue := dbos.NewWorkflowQueue(ctx, "rate-limited-queue",
		dbos.WithRateLimiter(&dbos.RateLimiter{
			Limit:  5,    // Max 5 tasks
			Period: 10.0, // Per 10 seconds
		}),
		dbos.WithWorkerConcurrency(1),
	)

	// Launch DBOS runtime
	err = ctx.Launch()
	if err != nil {
		log.Fatalf("Failed to launch DBOS: %v", err)
	}
	defer ctx.Cancel()

	fmt.Println("DBOS Queues Example Started")
	fmt.Println("=====================================")

	// Example 1: Standard Queue
	fmt.Println("\n📋 Example 1: Standard Queue (max 2 concurrent)")
	fmt.Println("Enqueuing 5 tasks...")
	
	standardHandles := make([]dbos.WorkflowHandle[string], 5)
	for i := 0; i < 5; i++ {
		task := TaskData{ID: i + 1, Name: fmt.Sprintf("Standard Task %d", i+1)}
		handle, err := dbos.RunAsWorkflow(ctx, processTask, task, 
			dbos.WithQueue(standardQueue.Name))
		if err != nil {
			log.Printf("Failed to enqueue task %d: %v", i+1, err)
			continue
		}
		standardHandles[i] = handle
		fmt.Printf("Enqueued task %d\n", i+1)
	}

	// Example 2: Priority Queue
	fmt.Println("\n⭐ Example 2: Priority Queue")
	fmt.Println("Enqueuing tasks with different priorities...")
	
	priorityHandles := make([]dbos.WorkflowHandle[string], 3)
	priorities := []uint{10, 1, 5} // Lower number = higher priority
	
	for i, priority := range priorities {
		task := TaskData{ID: 100 + i, Name: fmt.Sprintf("Priority Task (pri=%d)", priority)}
		handle, err := dbos.RunAsWorkflow(ctx, processTask, task,
			dbos.WithQueue(priorityQueue.Name),
			dbos.WithPriority(priority))
		if err != nil {
			log.Printf("Failed to enqueue priority task: %v", err)
			continue
		}
		priorityHandles[i] = handle
		fmt.Printf("Enqueued task with priority %d\n", priority)
	}

	// Example 3: Rate-Limited Queue
	fmt.Println("\n⏱️ Example 3: Rate-Limited Queue (5 per 10 seconds)")
	fmt.Println("Enqueuing 8 tasks (will be rate-limited)...")
	
	rateLimitedHandles := make([]dbos.WorkflowHandle[string], 8)
	for i := 0; i < 8; i++ {
		task := TaskData{ID: 200 + i, Name: fmt.Sprintf("Rate-Limited Task %d", i+1)}
		handle, err := dbos.RunAsWorkflow(ctx, processTask, task,
			dbos.WithQueue(rateLimitedQueue.Name))
		if err != nil {
			log.Printf("Failed to enqueue rate-limited task: %v", err)
			continue
		}
		rateLimitedHandles[i] = handle
		fmt.Printf("Enqueued rate-limited task %d\n", i+1)
	}

	// Wait for and display results
	fmt.Println("\n📊 Waiting for all tasks to complete...")
	fmt.Println("(Watch how tasks are processed according to queue configurations)")
	
	// Standard queue results
	fmt.Println("\n--- Standard Queue Results ---")
	for i, handle := range standardHandles {
		if handle == nil {
			continue
		}
		result, err := handle.GetResult()
		if err != nil {
			log.Printf("Task %d failed: %v", i+1, err)
		} else {
			fmt.Printf("✓ %s\n", result)
		}
	}

	// Priority queue results
	fmt.Println("\n--- Priority Queue Results ---")
	for i, handle := range priorityHandles {
		if handle == nil {
			continue
		}
		result, err := handle.GetResult()
		if err != nil {
			log.Printf("Priority task %d failed: %v", i, err)
		} else {
			fmt.Printf("✓ %s\n", result)
		}
	}

	// Rate-limited queue results
	fmt.Println("\n--- Rate-Limited Queue Results ---")
	for i, handle := range rateLimitedHandles {
		if handle == nil {
			continue
		}
		result, err := handle.GetResult()
		if err != nil {
			log.Printf("Rate-limited task %d failed: %v", i+1, err)
		} else {
			fmt.Printf("✓ %s\n", result)
		}
	}

	fmt.Println("\n✅ Durable Queues Example Complete!")
	fmt.Println("Queues provide flow control, priority scheduling, and rate limiting.")
	fmt.Println("All queue state is stored in Postgres - no separate message broker needed!")
}