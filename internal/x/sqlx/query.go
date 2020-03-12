package sqlx

import (
	"context"
	"database/sql"
)

// Query executes a query on the given DB.
func Query(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) *sql.Rows {
	rows, err := db.QueryContext(ctx, query, args...)
	Must(err)
	return rows
}

// QueryManyN executes a query on the given DB and returns a slice of integer
// results.
func QueryManyN(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) []uint64 {
	rows := Query(ctx, db, query, args...)
	defer rows.Close()

	var (
		value  uint64
		result []uint64
	)

	for rows.Next() {
		Scan(rows, &value)
		result = append(result, value)
	}

	return result
}

// QueryManyS executes a query on the given DB and returns a slice of string
// results.
func QueryManyS(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) []string {
	rows := Query(ctx, db, query, args...)
	defer rows.Close()

	var (
		value  string
		result []string
	)

	for rows.Next() {
		Scan(rows, &value)
		result = append(result, value)
	}

	return result
}

// QueryN executes a query on the given DB and returns a single integer result.
func QueryN(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) uint64 {
	return ScanN(
		db.QueryRowContext(ctx, query, args...),
	)
}

// TryQueryN executes a query on the given DB and returns a single integer
// result.
//
// It returns false if there are no rows.
func TryQueryN(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) (uint64, bool) {
	return TryScanN(
		db.QueryRowContext(ctx, query, args...),
	)
}

// Scan scans the values from s into dest, or panics if unable to do so.
func Scan(s Scanner, dest ...interface{}) {
	Must(s.Scan(dest...))
}

// TryScan scans the values from s into dest, or panics if unable to do so.
//
// It returns false if there were no rows.
func TryScan(s Scanner, dest ...interface{}) bool {
	err := s.Scan(dest...)
	if err == sql.ErrNoRows {
		return false
	}

	Must(err)

	return true
}

// ScanN scans a single int value from s and returns it.
func ScanN(s Scanner) uint64 {
	var v uint64
	Scan(s, &v)
	return v
}

// TryScanN scans a single int value from s and returns it.
//
// It returns false if there were no rows.
func TryScanN(s Scanner) (uint64, bool) {
	var v uint64
	return v, TryScan(s, &v)
}
