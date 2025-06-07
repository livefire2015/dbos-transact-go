package dbos

import (
	"context"
)

type WorkflowParams struct {
	WorkflowID string
}

type WorkflowHandle[R any] interface {
	GetResult() (R, error)
}

// workflowHandle is a concrete implementation of WorkflowHandle
type workflowHandle[R any] struct {
	resultChan chan R
	errorChan  chan error
	ctx        context.Context
}

// GetResult waits for the workflow to complete and returns the result
func (h *workflowHandle[R]) GetResult() (R, error) {
	select {
	case result := <-h.resultChan:
		return result, nil
	case err := <-h.errorChan:
		var zero R
		return zero, err
	case <-h.ctx.Done():
		var zero R
		return zero, h.ctx.Err()
	}
}

type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)

// TODO: name can be found using reflection
func WithWorkflow[P any, R any](name string, fn WorkflowFunc[P, R]) func(ctx context.Context, input P) WorkflowHandle[R] {
	registerWorkflow(name, fn)
	return func(ctx context.Context, input P) WorkflowHandle[R] {
		return runAsWorkflow(ctx, fn, input)
	}
}

func runAsWorkflow[P any, R any](ctx context.Context, fn WorkflowFunc[P, R], input P) WorkflowHandle[R] {
	// Channel to receive the result from the goroutine
	resultChan := make(chan R, 1)
	errorChan := make(chan error, 1)

	// Create the handle
	handle := &workflowHandle[R]{
		resultChan: resultChan,
		errorChan:  errorChan,
		ctx:        ctx,
	}

	// Run the function in a goroutine
	go func() {
		result, err := fn(ctx, input)
		if err != nil {
			errorChan <- err
		} else {
			resultChan <- result
		}
	}()

	return handle
}
