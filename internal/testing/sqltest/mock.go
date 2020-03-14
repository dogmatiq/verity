package sqltest

import (
	"database/sql"
	"database/sql/driver"
	"sync"
)

// MockConnector is mock of the driver.Connector interface.
type MockConnector struct {
	driver.Connector
}

// Driver returns the connector's driver.
func (*MockConnector) Driver() driver.Driver {
	return &MockDriver{}
}

// MockDriver is mock of the driver.Driver interface.
type MockDriver struct {
	driver.Driver
}

// MockDB returns a database pool that uses the mock connector.
func MockDB() *sql.DB {
	return sql.OpenDB(&MockConnector{})
}

var once sync.Once

// MockDriverName returns the mock driver name for use with sql.Open().
func MockDriverName() string {
	n := "infix-mock"

	once.Do(func() {
		sql.Register(n, &MockDriver{})
	})

	return n
}
