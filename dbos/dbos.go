package dbos

import (
	"fmt"
)

// DBOS represents the main DBOS instance
type DBOS struct {
	// XXX does this need to be private?
	systemDB SystemDatabase
}

// New creates a new DBOS instance with an initialized system database
func New() (*DBOS, error) {
	// Create the system database
	systemDB, err := NewSystemDatabase()
	if err != nil {
		return nil, fmt.Errorf("failed to create system database: %w", err)
	}

	return &DBOS{
		systemDB: systemDB,
	}, nil
}

// Close closes the DBOS instance and its resources
// TODO: rename destroy
func (d *DBOS) Close() error {
	if d.systemDB != nil {
		return d.systemDB.Close()
	}
	return nil
}

// SystemDB returns the system database instance
func (d *DBOS) SystemDB() SystemDatabase {
	return d.systemDB
}
