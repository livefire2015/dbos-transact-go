package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-go/dbos"
)

// sendWorkflow sends a message to another workflow
func sendWorkflow(ctx dbos.DBOSContext, message string) (string, error) {
	fmt.Printf("📤 Sender: Sending message '%s' to receiver\n", message)
	
	err := dbos.Send(ctx, dbos.GenericWorkflowSendInput[string]{
		DestinationID: "receiverID",
		Topic:         "topic",
		Message:       message,
	})
	if err != nil {
		return "", fmt.Errorf("failed to send message: %w", err)
	}
	
	return fmt.Sprintf("Message '%s' sent successfully", message), nil
}

// receiveWorkflow waits for and receives a message
func receiveWorkflow(ctx dbos.DBOSContext, topic string) (string, error) {
	fmt.Printf("📥 Receiver: Waiting for message on topic '%s' (timeout: 48 hours)...\n", topic)
	
	message, err := dbos.Recv[string](ctx, dbos.WorkflowRecvInput{
		Topic:   topic,
		Timeout: 48 * time.Hour,
	})
	if err != nil {
		return "", fmt.Errorf("failed to receive message: %w", err)
	}
	
	fmt.Printf("📥 Receiver: Got message: '%s'\n", message)
	return fmt.Sprintf("Received: %s", message), nil
}

// Payment workflow example - demonstrates real-world notification use case
type PaymentRequest struct {
	OrderID    string
	CustomerID string
	Amount     float64
}

// paymentWorkflow processes a payment and waits for confirmation
func paymentWorkflow(ctx dbos.DBOSContext, request PaymentRequest) (string, error) {
	fmt.Printf("\n💳 Payment Workflow: Processing order %s for customer %s ($%.2f)\n",
		request.OrderID, request.CustomerID, request.Amount)
	
	// Step 1: Initiate payment
	_, err := dbos.RunAsStep(ctx, func(ctx2 context.Context) (string, error) {
		fmt.Printf("  Step 1: Initiating payment for order %s\n", request.OrderID)
		// Simulate payment initiation
		return "Payment initiated", nil
	})
	if err != nil {
		return "", err
	}

	// Step 2: Wait for payment confirmation (with timeout)
	fmt.Printf("  Step 2: Waiting for payment confirmation (timeout: 30 seconds)...\n")
	confirmation, err := dbos.Recv[string](ctx, dbos.WorkflowRecvInput{
		Topic:   fmt.Sprintf("payment_%s", request.OrderID),
		Timeout: 30 * time.Second,
	})
	if err != nil {
		fmt.Printf("  ⏱️ Payment confirmation timeout for order %s\n", request.OrderID)
		return fmt.Sprintf("Payment timeout for order %s", request.OrderID), nil
	}
	
	fmt.Printf("  ✅ Payment confirmed: %s\n", confirmation)
	
	// Step 3: Complete order
	result, err := dbos.RunAsStep(ctx, func(ctx2 context.Context) (string, error) {
		fmt.Printf("  Step 3: Completing order %s\n", request.OrderID)
		return fmt.Sprintf("Order %s completed successfully", request.OrderID), nil
	})
	if err != nil {
		return "", err
	}
	
	return result, nil
}

// paymentConfirmationService simulates an external payment service
func paymentConfirmationService(ctx dbos.DBOSContext, orderID string) (string, error) {
	fmt.Printf("🏦 Payment Service: Processing confirmation for order %s\n", orderID)
	
	// Simulate payment processing delay
	ctx.Sleep(3 * time.Second)
	
	// Send confirmation to the waiting payment workflow
	err := dbos.Send(ctx, dbos.GenericWorkflowSendInput[string]{
		DestinationID: fmt.Sprintf("payment_workflow_%s", orderID),
		Topic:         fmt.Sprintf("payment_%s", orderID),
		Message:       fmt.Sprintf("Payment confirmed for order %s", orderID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to send confirmation: %w", err)
	}
	
	return fmt.Sprintf("Confirmation sent for order %s", orderID), nil
}

// Event-based workflow coordination example
func coordinatorWorkflow(ctx dbos.DBOSContext, taskCount int) (string, error) {
	fmt.Printf("\n🎯 Coordinator: Starting %d worker tasks\n", taskCount)
	
	// Start worker tasks
	for i := 1; i <= taskCount; i++ {
		workerID := fmt.Sprintf("worker_%d", i)
		_, err := dbos.RunAsWorkflow(ctx, workerTask, i, dbos.WithWorkflowID(workerID))
		if err != nil {
			log.Printf("Failed to start worker %d: %v", i, err)
		}
	}
	
	// Wait for all workers to complete
	fmt.Println("  Waiting for workers to send completion notifications...")
	completedCount := 0
	for completedCount < taskCount {
		notification, err := dbos.Recv[string](ctx, dbos.WorkflowRecvInput{
			Topic:   "worker_complete",
			Timeout: 30 * time.Second,
		})
		if err != nil {
			fmt.Printf("  ⏱️ Timeout waiting for worker completion\n")
			break
		}
		completedCount++
		fmt.Printf("  ✓ Received: %s (%d/%d)\n", notification, completedCount, taskCount)
	}
	
	return fmt.Sprintf("Coordination complete: %d/%d workers finished", completedCount, taskCount), nil
}

// workerTask simulates a worker that notifies the coordinator when done
func workerTask(ctx dbos.DBOSContext, workerID int) (string, error) {
	fmt.Printf("  👷 Worker %d: Starting task\n", workerID)
	
	// Simulate work with varying duration
	workDuration := time.Duration(workerID) * time.Second
	ctx.Sleep(workDuration)
	
	// Notify coordinator of completion
	err := dbos.Send(ctx, dbos.GenericWorkflowSendInput[string]{
		DestinationID: "coordinator",
		Topic:         "worker_complete",
		Message:       fmt.Sprintf("Worker %d completed", workerID),
	})
	if err != nil {
		return "", fmt.Errorf("failed to notify coordinator: %w", err)
	}
	
	return fmt.Sprintf("Worker %d finished after %s", workerID, workDuration), nil
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
		AppName:     "notifications-example",
	})
	if err != nil {
		log.Fatalf("Failed to create DBOS context: %v", err)
	}

	// Register workflows
	dbos.RegisterWorkflow(ctx, sendWorkflow)
	dbos.RegisterWorkflow(ctx, receiveWorkflow)
	dbos.RegisterWorkflow(ctx, paymentWorkflow)
	dbos.RegisterWorkflow(ctx, paymentConfirmationService)
	dbos.RegisterWorkflow(ctx, coordinatorWorkflow)
	dbos.RegisterWorkflow(ctx, workerTask)

	// Launch DBOS runtime
	err = ctx.Launch()
	if err != nil {
		log.Fatalf("Failed to launch DBOS: %v", err)
	}
	defer ctx.Cancel()

	fmt.Println("DBOS Durable Notifications Example")
	fmt.Println("=====================================")

	// Example 1: Basic Send/Receive
	fmt.Println("\n📌 Example 1: Basic Send/Receive Pattern")
	fmt.Println("Starting a receiver workflow and sending it a message...")
	
	// Start a receiver in the background
	recvHandle, err := dbos.RunAsWorkflow(ctx, receiveWorkflow, "topic", 
		dbos.WithWorkflowID("receiverID"))
	if err != nil {
		log.Fatalf("Failed to start receiver: %v", err)
	}
	
	// Give receiver time to start
	time.Sleep(100 * time.Millisecond)
	
	// Send a message
	sendHandle, err := dbos.RunAsWorkflow(ctx, sendWorkflow, "Hello from DBOS!")
	if err != nil {
		log.Fatalf("Failed to start sender: %v", err)
	}
	
	// Get sender result
	sendResult, err := sendHandle.GetResult()
	if err != nil {
		log.Printf("Send failed: %v", err)
	} else {
		fmt.Printf("Sender result: %s\n", sendResult)
	}
	
	// Get receiver result
	recvResult, err := recvHandle.GetResult()
	if err != nil {
		log.Printf("Receive failed: %v", err)
	} else {
		fmt.Printf("Receiver result: %s\n", recvResult)
	}

	// Example 2: Payment Processing with Confirmation
	fmt.Println("\n📌 Example 2: Payment Processing with Confirmation")
	
	payments := []PaymentRequest{
		{OrderID: "ORD-001", CustomerID: "CUST-123", Amount: 99.99},
		{OrderID: "ORD-002", CustomerID: "CUST-456", Amount: 149.99},
	}
	
	for _, payment := range payments {
		// Start payment workflow
		paymentHandle, err := dbos.RunAsWorkflow(ctx, paymentWorkflow, payment,
			dbos.WithWorkflowID(fmt.Sprintf("payment_workflow_%s", payment.OrderID)))
		if err != nil {
			log.Printf("Failed to start payment workflow: %v", err)
			continue
		}
		
		// Simulate payment service confirming after a delay
		go func(orderID string) {
			time.Sleep(1 * time.Second)
			_, err := dbos.RunAsWorkflow(ctx, paymentConfirmationService, orderID)
			if err != nil {
				log.Printf("Failed to confirm payment: %v", err)
			}
		}(payment.OrderID)
		
		// Wait for payment workflow to complete
		result, err := paymentHandle.GetResult()
		if err != nil {
			log.Printf("Payment workflow failed: %v", err)
		} else {
			fmt.Printf("Payment result: %s\n", result)
		}
	}

	// Example 3: Coordinator-Worker Pattern
	fmt.Println("\n📌 Example 3: Coordinator-Worker Pattern")
	fmt.Println("Starting coordinator with 3 workers...")
	
	coordHandle, err := dbos.RunAsWorkflow(ctx, coordinatorWorkflow, 3,
		dbos.WithWorkflowID("coordinator"))
	if err != nil {
		log.Fatalf("Failed to start coordinator: %v", err)
	}
	
	coordResult, err := coordHandle.GetResult()
	if err != nil {
		log.Printf("Coordinator failed: %v", err)
	} else {
		fmt.Printf("Coordinator result: %s\n", coordResult)
	}


	fmt.Println("\n✅ Durable Notifications Example Complete!")
	fmt.Println("Key features demonstrated:")
	fmt.Println("• Send/Receive pattern for workflow communication")
	fmt.Println("• Payment confirmation with timeout handling")
	fmt.Println("• Coordinator-worker pattern for distributed tasks")
	fmt.Println("• All notifications stored durably in Postgres with exactly-once semantics")
}