package dbos

import (
	"fmt"
)

type Executor interface {
	Destroy() error
}

// DBOS represents the main DBOS instance
type executor struct {
	systemDB SystemDatabase
}

// New creates a new DBOS instance with an initialized system database
var dbos *executor

func getExecutor() *executor {
	// TODO find a good strategy
	if dbos == nil {
		panic("DBOS instance is not initialized")
	}
	return dbos
}

func Launch() error {
	if dbos != nil {
		// XXX: maybe just log a warning instead of returning an error
		return fmt.Errorf("DBOS already initialized")
	}
	// Create the system database
	systemDB, err := NewSystemDatabase()
	if err != nil {
		return fmt.Errorf("failed to create system database: %w", err)
	}

	dbos = &executor{
		systemDB: systemDB,
	}
	return nil
}

// Close closes the DBOS instance and its resources
// TODO: rename destroy
func Destroy() error {
	if dbos == nil {
		// FIXME: just emit a warning
		return fmt.Errorf("DBOS instance is nil, cannot destroy")
	}
	if dbos.systemDB != nil {
		return dbos.systemDB.Destroy()
	}
	return nil
}
