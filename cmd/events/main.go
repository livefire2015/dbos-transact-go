package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-go/dbos"
)

// WebhookEvent represents an incoming webhook event
type WebhookEvent struct {
	EventID     string
	EventType   string
	CustomerID  string
	Amount      float64
	Timestamp   time.Time
}

// processPaymentEvent handles payment webhook events with exactly-once semantics
func processPaymentEvent(ctx dbos.DBOSContext, event WebhookEvent) (string, error) {
	fmt.Printf("Processing payment event: %s for customer %s\n", event.EventID, event.CustomerID)
	
	// Step 1: Validate the event
	_, err := dbos.RunAsStep(ctx, func(ctx2 context.Context) (bool, error) {
		fmt.Printf("  Validating event %s...\n", event.EventID)
		// Simulate validation logic
		if event.Amount <= 0 {
			return false, fmt.Errorf("invalid amount: %f", event.Amount)
		}
		return true, nil
	})
	if err != nil {
		return "", fmt.Errorf("validation failed: %w", err)
	}

	// Step 2: Process the payment
	_, err = dbos.RunAsStep(ctx, func(ctx2 context.Context) (string, error) {
		fmt.Printf("  Processing payment of $%.2f for customer %s\n", event.Amount, event.CustomerID)
		// Simulate payment processing
		time.Sleep(1 * time.Second)
		return fmt.Sprintf("PAYMENT_%s_PROCESSED", event.EventID), nil
	})
	if err != nil {
		return "", fmt.Errorf("payment processing failed: %w", err)
	}

	// Step 3: Update customer records
	result, err := dbos.RunAsStep(ctx, func(ctx2 context.Context) (string, error) {
		fmt.Printf("  Updating customer %s records\n", event.CustomerID)
		// Simulate database update
		return fmt.Sprintf("Customer %s updated with payment $%.2f", event.CustomerID, event.Amount), nil
	})
	if err != nil {
		return "", fmt.Errorf("customer update failed: %w", err)
	}

	// Step 4: Send confirmation (could be email, SMS, etc.)
	_, err = dbos.RunAsStep(ctx, func(ctx2 context.Context) (bool, error) {
		fmt.Printf("  Sending confirmation for event %s\n", event.EventID)
		// Simulate sending confirmation
		return true, nil
	})
	if err != nil {
		return "", fmt.Errorf("confirmation failed: %w", err)
	}

	return fmt.Sprintf("Event %s processed successfully: %s", event.EventID, result), nil
}

// processOrderEvent handles order events with exactly-once semantics
func processOrderEvent(ctx dbos.DBOSContext, event WebhookEvent) (string, error) {
	fmt.Printf("Processing order event: %s for customer %s\n", event.EventID, event.CustomerID)
	
	// Simulate order processing workflow
	ctx.Sleep(500 * time.Millisecond)
	
	// Set an event that other workflows can read
	err := dbos.SetEvent(ctx, dbos.GenericWorkflowSetEventInput[string]{
		Key:   "order_status",
		Message: fmt.Sprintf("Order %s completed", event.EventID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to set event: %w", err)
	}
	
	return fmt.Sprintf("Order %s processed for customer %s", event.EventID, event.CustomerID), nil
}

// simulateWebhookReceiver simulates receiving webhook events
func simulateWebhookReceiver(ctx dbos.DBOSContext) {
	// Simulate receiving multiple webhook events
	events := []WebhookEvent{
		{
			EventID:    "PAY_001",
			EventType:  "payment",
			CustomerID: "CUST_123",
			Amount:     99.99,
			Timestamp:  time.Now(),
		},
		{
			EventID:    "PAY_001", // Duplicate event ID - will be processed exactly once
			EventType:  "payment",
			CustomerID: "CUST_123",
			Amount:     99.99,
			Timestamp:  time.Now(),
		},
		{
			EventID:    "ORD_001",
			EventType:  "order",
			CustomerID: "CUST_456",
			Amount:     249.99,
			Timestamp:  time.Now(),
		},
		{
			EventID:    "PAY_002",
			EventType:  "payment",
			CustomerID: "CUST_789",
			Amount:     149.99,
			Timestamp:  time.Now(),
		},
		{
			EventID:    "ORD_001", // Duplicate order - will be processed exactly once
			EventType:  "order",
			CustomerID: "CUST_456",
			Amount:     249.99,
			Timestamp:  time.Now(),
		},
	}

	fmt.Println("\n📨 Simulating webhook events...")
	fmt.Println("(Notice how duplicate events are handled exactly-once)")
	
	for _, event := range events {
		// Generate a unique workflow ID based on event ID to ensure exactly-once processing
		workflowID := fmt.Sprintf("event_%s_%s", event.EventType, event.EventID)
		
		fmt.Printf("\n→ Received %s event: %s\n", event.EventType, event.EventID)
		
		// Route to appropriate handler based on event type
		switch event.EventType {
		case "payment":
			handle, err := dbos.RunAsWorkflow(ctx, processPaymentEvent, event,
				dbos.WithWorkflowID(workflowID)) // Using event ID ensures exactly-once processing
			if err != nil {
				// Check if it's because workflow already exists (duplicate event)
				if handle != nil {
					fmt.Printf("  ⚠️ Event %s already being processed (exactly-once guarantee)\n", event.EventID)
					// We can still get the result of the existing workflow
					result, _ := handle.GetResult()
					fmt.Printf("  Existing result: %s\n", result)
				} else {
					log.Printf("Failed to process payment event: %v", err)
				}
			} else {
				result, err := handle.GetResult()
				if err != nil {
					log.Printf("Payment processing failed: %v", err)
				} else {
					fmt.Printf("  ✓ %s\n", result)
				}
			}
			
		case "order":
			handle, err := dbos.RunAsWorkflow(ctx, processOrderEvent, event,
				dbos.WithWorkflowID(workflowID))
			if err != nil {
				if handle != nil {
					fmt.Printf("  ⚠️ Event %s already being processed (exactly-once guarantee)\n", event.EventID)
					result, _ := handle.GetResult()
					fmt.Printf("  Existing result: %s\n", result)
				} else {
					log.Printf("Failed to process order event: %v", err)
				}
			} else {
				result, err := handle.GetResult()
				if err != nil {
					log.Printf("Order processing failed: %v", err)
				} else {
					fmt.Printf("  ✓ %s\n", result)
				}
			}
		}
		
		// Small delay between events for readability
		time.Sleep(100 * time.Millisecond)
	}
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
		AppName:     "events-example",
	})
	if err != nil {
		log.Fatalf("Failed to create DBOS context: %v", err)
	}

	// Register workflows
	dbos.RegisterWorkflow(ctx, processPaymentEvent)
	dbos.RegisterWorkflow(ctx, processOrderEvent)

	// Launch DBOS runtime
	err = ctx.Launch()
	if err != nil {
		log.Fatalf("Failed to launch DBOS: %v", err)
	}
	defer ctx.Cancel()

	fmt.Println("DBOS Exactly-Once Event Processing Example")
	fmt.Println("==========================================")
	fmt.Println("This example demonstrates how to process webhook events")
	fmt.Println("with exactly-once semantics using workflow IDs.")

	// Simulate receiving webhook events
	simulateWebhookReceiver(ctx)

	fmt.Println("\n✅ Exactly-Once Event Processing Example Complete!")
	fmt.Println("Key features demonstrated:")
	fmt.Println("• Duplicate events are automatically detected and handled")
	fmt.Println("• Each event is processed exactly once, even if received multiple times")
	fmt.Println("• Workflow state is preserved across restarts")
	fmt.Println("• Perfect for webhooks, Kafka consumers, and event-driven architectures")
}