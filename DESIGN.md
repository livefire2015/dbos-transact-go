# Outline
- library structure
- library initialization
- the public interface:
    * wrappers
    * WorkflowStatus
    * WorkflowHandle
    * Config object (or options setting function)
    * Client
- wrapping functions as durable workflows
- workflow handles
- handling user-provided datasources
- system database
- user database
- global parameters
- docs
- releases

Workflow Handles
---
Regardless of the wrapping pattern, wrapped functions will return handles

```golang
type WorkflowHandle interface {
    GetResult() []any
    Status() WorkflowStatus
    Cancel()
}
```

Wrapping functions in Durable Workflows
---

I. Patterns

1. Decorator pattern

```golang
type WorkflowParams struct {
    timeout time.Duration
}

type WorkflowHandle interface {
    GetResult() ([]any, error)
    Cancel() error
}

func WithWorkflow(fn any) func(context.Context, ...any) (WorkflowHandle, error) {
    return func(ctx context.Context, params WorkflowParams, args ...any) (WorkflowHandle, error) {
        return internalWorkflow(ctx, fn, args, params) // WILL RETURN HANDLE
    }
}

// Usage:
package userpackage

var (
    wrappedFunc := WithWorkflow(userFunction,)
)

func userFunction(ctx context.Context, arg string) (string, error) {
    return "yes!", nil
}

type myService struct {
    myAPIKey int
}

func (s *myService) myFuncton() {
    handle, err := wrappedFunc(ctx, s.myAPIKey, WorkflowParams{timeout: time.Minute * 10})
    results, err := handle.GetResult()
}
```

2. Interface pattern: rejected because too close to the Proxy Object pattern, more heavyweight.

II. Signatures

1. Generics

Go doesn't support (yet) variadic generic parameters, so for example we cannot do:
```golang
func WithWorkflow[T1, T2, ...TN any, ...RN any](fn func(T1, T2, ...TN) (R1, R2, ...RN)) // ❌ Can't do this
```
Support for generic is limited to fixed number of parameters, which is prohibitive in our use case.

2. Typeless wrappers

To capture arbitrary user-defined functions, we must use a typeless signature and use Reflection to validate and call the user code. Take this simplified wrapper that accepts a user function and returns a wrapped function that directly calls the user code:

```golang
  func WithWorkflow(fn any) any {
    // Check the type and cache it in registry
    return func(args ...any) []any {
        // is fn a function? What's the signature
        // For each args in signature
        // What's the type?
        // Is number of args in signature == len(args)

        return fn(args) // ❌ Cannot call the function directly, need Reflection
    }
  }
```

`fn` is not directly usable and must be thoroughly validated first. Reflection introduces performance overheads (under 1 millisecond) which can be optimized with caching (to be explored).

3. Constrained types
To iterate faster, we settled on this

```golang
type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)

func WithWorkflow[P any, R any](name string, fn WorkflowFunc[P, R]) func(ctx context.Context, input P, params WorkflowParams) WorkflowHandle[R]
```

Registry
---

Go doesn't have decorators, which are essentially interpreted either by the compiler or while loading the code (i.e., before the programs actually run). A curated list of our best options:
- Ask users to wrap their workflow in package level variables. Because these are evaluated even before init() runs, the registry would be maintained inside the wrapper. A variant to this approach is to ask them to write an init() function that does the wrapping.
    pros: easier for us
    cons: not exactly "lightweight" from a user standpoint
- use go generate to, well, generate, at compile time, an init() function that builds the registry. The generator will parse the AST to lookup for our wrapper functions. This also requires the user to run go:generate before building their program.
    pros: easier for the user
    cons: more complex to write the registering function

--> init() functions execute in alpha/lexical order within a package. We *might* want to take an "early" name but I don't think it'd be necessary?

Executing user code
---
- In their own goroutine with an execution wrapper that'll intercept exceptions
- The goroutine will write the results to a channel?
- The handler will have access to a channel with the goroutine to check on result?
- We will see the goroutine wrapper with our context object that'll have, e.g., the deadline


Contexts
---
- Do we implement our own Context?

Serializing inputs/outputs
---

Database management
---
- Migrations
- Drivers
- How to receive a user provided datasources
    * checkout https://github.com/dbos-inc/dbos-transact-ts/pull/951/files

Config
---

Logging
---

Tracing
---

Determinism
---
- Concurrent go routines will run in a non-deterministic order
- 	for key, value := range data { // where data map[string]int is non-deterministic
- select choses randomly in a list of channels
- select based on a "race" between multiple goroutines (like Promise.race() or asyncio.gather())
- Ofc ASLR, random numbers, etc


Command line
---

Client
---


Prepare early version for prospects review
- Ask prospects about quicks