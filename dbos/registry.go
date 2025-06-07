package dbos

import (
	"fmt"
	"sync"
)

var registry = make(map[string]any)
var regMutex sync.RWMutex

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(name string, fn any) {
	regMutex.Lock()
	defer regMutex.Unlock()

	if _, exists := registry[name]; exists {
		panic(fmt.Sprintf("workflow function '%s' is already registered", name))
	}

	registry[name] = fn
}
