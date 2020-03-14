package driver_test

import (
	"database/sql"

	"github.com/dogmatiq/infix/internal/testing/sqltest"
	. "github.com/dogmatiq/infix/persistence/provider/sql/driver"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/mysql"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/postgres"
	"github.com/dogmatiq/infix/persistence/provider/sql/driver/sqlite"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = Describe("func New()", func() {
	DescribeTable(
		"it returns the expected driver",
		func(name, dsn string, expected *Driver) {
			db, err := sql.Open(name, dsn)
			Expect(err).ShouldNot(HaveOccurred())
			defer db.Close()

			d, err := New(db)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(d).To(Equal(expected))
		},
		Entry(
			"mysql", "mysql", "tcp(127.0.0.1)/mysql",
			&Driver{
				StreamDriver: mysql.StreamDriver{},
			},
		),
		Entry(
			"postgres", "postgres", "host=localhost",
			&Driver{
				StreamDriver: postgres.StreamDriver{},
			},
		),
		Entry(
			"sqlite", "sqlite3", ":memory:",
			&Driver{
				StreamDriver: sqlite.StreamDriver{},
			},
		),
	)

	It("returns an error if the driver is unrecognised", func() {
		_, err := New(sqltest.MockDB())
		Expect(err).Should(HaveOccurred())
	})
})
