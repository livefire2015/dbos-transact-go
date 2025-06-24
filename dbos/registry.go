package dbos

import (
	"context"
	"fmt"
	"sync"
)

type TypedErasedWorkflowWrapperFunc func(ctx context.Context, params WorkflowParams, input any) (WorkflowHandle[any], error)

var registry = make(map[string]TypedErasedWorkflowWrapperFunc)
var regMutex sync.RWMutex

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(fqdn string, fn TypedErasedWorkflowWrapperFunc) {
	regMutex.Lock()
	defer regMutex.Unlock()

	if _, exists := registry[fqdn]; exists {
		panic(fmt.Sprintf("workflow function '%s' is already registered", fqdn))
	}

	registry[fqdn] = fn
}
