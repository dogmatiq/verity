package sqlx

import (
	"context"
)

// QueryInto executes single-column, single-row query on the given DB and scans
// the result into a value.
func QueryInto(
	ctx context.Context,
	db DB,
	value interface{},
	query string,
	args ...interface{},
) {
	row := db.QueryRowContext(ctx, query, args...)
	Must(row.Scan(value))
}

// QueryInt64 executes a single-column, single-row query on the given DB and
// returns a single uint64 result.
func QueryInt64(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) (v int64) {
	QueryInto(ctx, db, &v, query, args...)
	return v
}

// QueryBool executes a single-column, single-row query on the given DB and
// returns a single bool result.
func QueryBool(
	ctx context.Context,
	db DB,
	query string,
	args ...interface{},
) (v bool) {
	QueryInto(ctx, db, &v, query, args...)
	return v
}

// Scan scans values from a row or row-set.
func Scan(rows Scanner, targets ...interface{}) {
	Must(rows.Scan(targets...))
}

// ScanInt64 scans a single int64 alue from a row or row-set.
func ScanInt64(rows Scanner) int64 {
	var v int64
	Scan(rows, &v)
	return v
}
