// Package streamfilter contains cross-database utilities limiting stream cursor
// results to specific message types by joing to a table of message type names.
//
// This approach is taken as an alternative to producing queries with
// potentially very large IN clauses, which often perform very poorly.
package streamfilter
