package dbos

import (
	"fmt"
	"reflect"
	"runtime"
	"sync"
)

var registry = make(map[string]any)
var regMutex sync.RWMutex

// Register adds a workflow function to the registry (thread-safe, only once per name)
func registerWorkflow(fn any) {
	fqdn := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	regMutex.Lock()
	defer regMutex.Unlock()

	if _, exists := registry[fqdn]; exists {
		panic(fmt.Sprintf("workflow function '%s' is already registered", fqdn))
	}

	registry[fqdn] = fn
}
