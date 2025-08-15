package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-go/dbos"
)

// scheduledHealthCheck runs periodic health checks
func scheduledHealthCheck(ctx dbos.DBOSContext, scheduledTime time.Time) (string, error) {
	fmt.Printf("🔍 Health check executed at %s (scheduled for %s)\n", 
		time.Now().Format("15:04:05"), scheduledTime.Format("15:04:05"))
	
	// Simulate health check
	status := "healthy"
	return fmt.Sprintf("System status: %s at %s", status, scheduledTime), nil
}

// scheduledDataSync performs periodic data synchronization
func scheduledDataSync(ctx dbos.DBOSContext, scheduledTime time.Time) (string, error) {
	fmt.Printf("🔄 Data sync started at %s\n", time.Now().Format("15:04:05"))
	
	// Simulate data sync process
	ctx.Sleep(2 * time.Second)
	
	return fmt.Sprintf("Data sync completed at %s", time.Now().Format("15:04:05")), nil
}

// scheduledReport generates reports at specific times
func scheduledReport(ctx dbos.DBOSContext, scheduledTime time.Time) (string, error) {
	fmt.Printf("📊 Generating report for %s\n", scheduledTime.Format("2006-01-02 15:04:05"))
	
	// Simulate report generation
	ctx.Sleep(1 * time.Second)
	
	return fmt.Sprintf("Report generated successfully at %s", time.Now().Format("15:04:05")), nil
}

// longRunningWorkflow demonstrates durable sleep
func longRunningWorkflow(ctx dbos.DBOSContext, config WorkflowConfig) (string, error) {
	fmt.Printf("Starting long-running workflow: %s\n", config.Name)
	
	// Step 1: Initial processing
	_, err := dbos.RunAsStep(ctx, func(ctx2 context.Context) (string, error) {
		fmt.Printf("  Step 1: Initial processing for %s\n", config.Name)
		return "Step 1 complete", nil
	})
	if err != nil {
		return "", err
	}

	// Durable sleep - survives restarts!
	fmt.Printf("  💤 Sleeping for %s (durable - survives restarts)...\n", config.SleepDuration)
	sleepDuration, err := ctx.Sleep(config.SleepDuration)
	if err != nil {
		return "", err
	}
	fmt.Printf("  ⏰ Woke up after %s!\n", sleepDuration)

	// Step 2: Processing after sleep
	result, err := dbos.RunAsStep(ctx, func(ctx2 context.Context) (string, error) {
		fmt.Printf("  Step 2: Final processing for %s\n", config.Name)
		return fmt.Sprintf("Workflow %s completed after sleeping %s", config.Name, config.SleepDuration), nil
	})
	if err != nil {
		return "", err
	}

	return result, nil
}

// WorkflowConfig holds configuration for long-running workflows
type WorkflowConfig struct {
	Name          string
	SleepDuration time.Duration
}

// delayedNotification sends a notification after a delay
func delayedNotification(ctx dbos.DBOSContext, notification NotificationConfig) (string, error) {
	fmt.Printf("📬 Scheduling notification: %s\n", notification.Message)
	
	// Wait for the specified delay
	if notification.Delay > 0 {
		fmt.Printf("  Waiting %s before sending...\n", notification.Delay)
		ctx.Sleep(notification.Delay)
	}
	
	// Send the notification
	fmt.Printf("  📤 Sending: %s to %s\n", notification.Message, notification.Recipient)
	
	return fmt.Sprintf("Notification sent to %s at %s", notification.Recipient, time.Now().Format("15:04:05")), nil
}

// NotificationConfig holds notification settings
type NotificationConfig struct {
	Recipient string
	Message   string
	Delay     time.Duration
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
		AppName:     "scheduling-example",
	})
	if err != nil {
		log.Fatalf("Failed to create DBOS context: %v", err)
	}

	// Register scheduled workflows with cron expressions
	
	// Run health check every 5 seconds
	dbos.RegisterWorkflow(ctx, scheduledHealthCheck, 
		dbos.WithSchedule("*/5 * * * * *")) // Every 5 seconds
	
	// Run data sync every 10 seconds
	dbos.RegisterWorkflow(ctx, scheduledDataSync,
		dbos.WithSchedule("*/10 * * * * *")) // Every 10 seconds
	
	// Run report generation every 15 seconds
	dbos.RegisterWorkflow(ctx, scheduledReport,
		dbos.WithSchedule("*/15 * * * * *")) // Every 15 seconds

	// Register non-scheduled workflows
	dbos.RegisterWorkflow(ctx, longRunningWorkflow)
	dbos.RegisterWorkflow(ctx, delayedNotification)

	// Launch DBOS runtime
	err = ctx.Launch()
	if err != nil {
		log.Fatalf("Failed to launch DBOS: %v", err)
	}
	defer ctx.Cancel()

	fmt.Println("DBOS Durable Scheduling Example")
	fmt.Println("=====================================")
	fmt.Println("Scheduled workflows:")
	fmt.Println("• Health checks: every 5 seconds")
	fmt.Println("• Data sync: every 10 seconds")
	fmt.Println("• Reports: every 15 seconds")
	fmt.Println()

	// Example 1: Long-running workflows with durable sleep
	fmt.Println("📌 Example 1: Long-Running Workflows with Durable Sleep")
	fmt.Println("Starting workflows that sleep for different durations...")
	
	workflows := []WorkflowConfig{
		{Name: "Quick Task", SleepDuration: 3 * time.Second},
		{Name: "Medium Task", SleepDuration: 7 * time.Second},
		{Name: "Long Task", SleepDuration: 10 * time.Second},
	}
	
	handles := make([]dbos.WorkflowHandle[string], len(workflows))
	for i, wf := range workflows {
		handle, err := dbos.RunAsWorkflow(ctx, longRunningWorkflow, wf)
		if err != nil {
			log.Printf("Failed to start workflow %s: %v", wf.Name, err)
			continue
		}
		handles[i] = handle
		fmt.Printf("  Started: %s (will sleep for %s)\n", wf.Name, wf.SleepDuration)
	}

	// Example 2: Delayed notifications
	fmt.Println("\n📌 Example 2: Delayed Notifications")
	notifications := []NotificationConfig{
		{Recipient: "user1@example.com", Message: "Immediate notification", Delay: 0},
		{Recipient: "user2@example.com", Message: "Reminder in 5 seconds", Delay: 5 * time.Second},
		{Recipient: "user3@example.com", Message: "Follow-up in 8 seconds", Delay: 8 * time.Second},
	}
	
	notifHandles := make([]dbos.WorkflowHandle[string], len(notifications))
	for i, notif := range notifications {
		handle, err := dbos.RunAsWorkflow(ctx, delayedNotification, notif)
		if err != nil {
			log.Printf("Failed to schedule notification: %v", err)
			continue
		}
		notifHandles[i] = handle
	}

	// Let scheduled workflows run for a bit
	fmt.Println("\n⏳ Scheduled workflows are running in the background...")
	fmt.Println("(Watch the periodic executions above)")
	time.Sleep(20 * time.Second)

	// Get results from long-running workflows
	fmt.Println("\n📊 Long-Running Workflow Results:")
	for i, handle := range handles {
		if handle == nil {
			continue
		}
		result, err := handle.GetResult()
		if err != nil {
			log.Printf("Workflow %d failed: %v", i, err)
		} else {
			fmt.Printf("  ✓ %s\n", result)
		}
	}

	// Get notification results
	fmt.Println("\n📊 Notification Results:")
	for i, handle := range notifHandles {
		if handle == nil {
			continue
		}
		result, err := handle.GetResult()
		if err != nil {
			log.Printf("Notification %d failed: %v", i, err)
		} else {
			fmt.Printf("  ✓ %s\n", result)
		}
	}

	fmt.Println("\n✅ Durable Scheduling Example Complete!")
	fmt.Println("Key features demonstrated:")
	fmt.Println("• Cron-based scheduled workflows")
	fmt.Println("• Durable sleep that survives restarts")
	fmt.Println("• Delayed task execution")
	fmt.Println("• All scheduling state persisted in Postgres")
	fmt.Println("\n💡 Try killing and restarting this program - scheduled workflows and sleeps will resume!")
}