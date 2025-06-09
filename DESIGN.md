# DBOS Transact Go - Design Document

## Outline

- [Library Overview](#library-overview)
- [Wrapping functions in Durable Workflows](#wrapping-functions)
- [DBOS Registry](#registry)
- [Executing User Code](#executing-user-code)
- [System Database](#system-database)
- [Client](#client)
- [Config](#config)
- [Logging](#logging)
- [Tracing](#tracing)
- [CLI](#cli)
- [docs](#docs)
- [Package Management](#package-management)
- [Golang quircks](#golang-quircks)

## Library overview

The library will require go 1.23.0 (we can discuss reducing this requirement)

The import path will be `github.com/dbos-inc/dbos-transact-go/dbos` (to comply both with the uniqueness of the name and go package naming conventions.)

```shell
.
├── dbos
│   ├── dbos_test.go
│   ├── dbos.go
│   ├── migrations
│   │   ├── 000001_initial_dbos_schema.down.sql
│   │   └── 000001_initial_dbos_schema.up.sql
│   ├── registry.go
│   ├── system_database.go
│   └── workflow.go
├── DESIGN.md
├── go.mod
├── go.sum
└── README.md
```

## Wrapping functions

### Overview

```golang
func WithWorkflow[P any, R any](name string, fn WorkflowFunc[P, R]) func(ctx context.Context, params WorkflowParams, input P) WorkflowHandle[R] {
	registerWorkflow(name, fn)
	return func(ctx context.Context, params WorkflowParams, input P) WorkflowHandle[R] {
		return runAsWorkflow(ctx, params, fn, input)
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

### Signatures

#### Generics

Go doesn't support (yet) variadic generic parameters, so for example we cannot do:
```golang
func WithWorkflow[T1, T2, ...TN any, ...RN any](fn func(T1, T2, ...TN) (R1, R2, ...RN)) // ❌ Can't do this
```
Support for generic is limited to fixed number of parameters, which is prohibitive in our use case.

#### Typeless wrappers

To capture arbitrary user-defined functions, we must use a typeless signature and use Reflection to validate and call the user code. Take this simplified wrapper that accepts a user function and returns a wrapped function that directly calls the user code:

```golang
  func WithWorkflow(fn any) any {
    // Check the type and cache it in registry
    return func(args ...any) []any {
        // is fn a function? What's the signature
        // Extract the type of each arg in the signature
        // Validate signature args type == calling args types
        // Validate number of args in signature == len(args)

        return fn(args) // ❌ Cannot call the function directly, need Reflection
    }
  }
```

`fn` is not directly usable and must be thoroughly validated first. Reflection introduces performance overheads (under 1 millisecond) which can be optimized with caching (to be explored).

#### Constrained types
To iterate faster, we settled on this:

```golang
type WorkflowFunc[P any, R any] func(ctx context.Context, input P) (R, error)

func WithWorkflow[P any, R any](name string, fn WorkflowFunc[P, R]) func(ctx context.Context, input P, params WorkflowParams) WorkflowHandle[R]
```

It does:
- Mandate a context is passed as the first argument
- The function takes a single input
- The function returns a single output and an error

Contexts are most useful in server programs, so not all programs use contexts everywhere. Interestingly "At Google, we require that Go programmers pass a Context parameter as the first argument to every function on the call path between incoming and outgoing requests.", quoted from the [go blog](https://go.dev/blog/context#conclusion). We can decide to *not* mandate this in the future, and rely on Reflection to determine whether a context was given.

Input and output parameters can be structures, so this remains flexible. Again, we can automate handling arbitrary numbers of input/output parameters with a typeless interface and runtime Reflection.


## Registry

Go doesn't have decorators, which can be used to perform stuff before the program actually runs, including registering operations.
A Go package initialization order is:
1. initiaze package variables
2. run an `init` function
3. run `main`

The easiest solution, which we started with, is to ask users to declare their wrapped function in a package variable.

We can automate this by writing a `go generate` script, which would parse the AST and generate an `init` function performing the registration, guaranteeing functions are registered before the program's `main` function is executed.

## Executing user code

We will run user provided functions in goroutines. In fact, we will need two goroutine:
1. A very simple wrapper than can run the user function and notify of success/error
2. A "monitor" goroutine that will handle the system database management and timeouts/cancellations on the context.

Note that Golang scheduling model is mostly cooperative, so even if we learn a goroutine's context is cancelled, we cannot, without user support, interrupt their execution.

We'll be able to do a few thing through the user function wrapper, for example handling panics to not crash the entire program.

### Contexts

We have two choices: either augment the user-provided context, or use a totally different one for DBOS operations. The former has the benefit of allowing a user to get access to DBOS metadata and other information natively, whereas the later allows a better decoupling of user code and Transact.

I'd favor the later for now.

#### Parent-Child relationships

We will derive children context from parent context -- easily. We'll just be careful to avoid "deep context chains".

## System Database
### Serializing inputs/outputs
TBD: settle on a library. There is one that's pickling in binary, pretty efficient, but not compatible across languages, which is not a concern for us today.

## Database management
We will use the golang-migrate package to automatically run migrations when a system database is created. The migrations are embedded in the program binary with `go:embed`.

WIP: How to receive user provided datasources.

## Client

## Config

## Logging
To explore: some loggers already support OTLP export, some require a "bridge". The idea should that we'll support users bringing their own logger for the 4 major loggers (logrus, zap, slog) and use them as an OTLP logger provider transparently.

## Tracing
- We'll export the tracer
- How will we play with the global tracer this time?

## Command line

## Docs

We will use go:doc to automatically generate the documentation

## Package management

The package will be "published" on its github repo. Package versions are managed with git tags.

## Golang quircks
### Determinism
- Concurrent go routines will run in a non-deterministic order
- 	for key, value := range data { // where data map[string]int is non-deterministic
- select choses randomly in a list of channels
- select based on a "race" between multiple goroutines (like Promise.race() or asyncio.gather())
- Ofc ASLR, random numbers, etc
